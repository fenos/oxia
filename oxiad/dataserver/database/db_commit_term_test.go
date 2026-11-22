// Copyright 2023-2026 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package database

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	commontime "github.com/oxia-db/oxia/common/time"
	"github.com/oxia-db/oxia/oxiad/common/errs"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"
	"github.com/oxia-db/oxia/oxiad/dataserver/wal"
)

func newCommitTermTestDB(t *testing.T) DB {
	t.Helper()
	factory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	db, err := NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_NATURAL, 0, commontime.SystemClock)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, db.Close())
		assert.NoError(t, factory.Close())
	})
	return db
}

func requireCommitEntry(t *testing.T, db DB, term int64, offset int64) {
	t.Helper()
	commit, err := db.ReadCommitEntryId()
	require.NoError(t, err)
	assert.Equal(t, term, commit.GetTerm(), "term")
	assert.Equal(t, offset, commit.GetOffset(), "offset")
}

// The database records the term of the entry it applied beside its offset,
// however the entry applies: a write, a control request, a write it rejects,
// or a write of a split child's parent. The term is the entry's own, not the
// term the replica is fenced at: a leader applies the entries of the terms
// before its own too.
func TestDBRecordsTheTermOfTheEntryItApplied(t *testing.T) {
	db := newCommitTermTestDB(t)
	requireCommitEntry(t, db, wal.InvalidTerm, wal.InvalidOffset)
	require.NoError(t, db.UpdateTerm(7, TermOptions{}))

	_, err := db.ProcessWrite(&proto.WriteRequest{Puts: []*proto.PutRequest{{Key: "a", Value: []byte("0")}}},
		3, 0, 0, NoOpCallback)
	require.NoError(t, err)
	requireCommitEntry(t, db, 3, 0)

	_, err = db.ProcessControlRequest(&proto.ControlRequest{
		Value: &proto.ControlRequest_RecordChecksum{RecordChecksum: &proto.RecordChecksumRequest{}},
	}, 4, 1, 0, NoOpCallback)
	require.NoError(t, err)
	requireCommitEntry(t, db, 4, 1)

	// Without the feature that validates them, a sequence key with fewer
	// deltas than the sequence has parts is rejected
	_, err = db.ProcessWrite(&proto.WriteRequest{Puts: []*proto.PutRequest{{
		Key: "s", Value: []byte("0"), PartitionKey: pb.String("x"), SequenceKeyDelta: []uint64{1, 2},
	}}}, 5, 2, 0, NoOpCallback)
	require.NoError(t, err)
	_, err = db.ProcessWrite(&proto.WriteRequest{Puts: []*proto.PutRequest{{
		Key: "s", Value: []byte("0"), PartitionKey: pb.String("x"), SequenceKeyDelta: []uint64{1},
	}}}, 6, 3, 0, NoOpCallback)
	require.ErrorIs(t, err, errs.ErrDBWriteRejected)
	requireCommitEntry(t, db, 6, 3)

	_, err = db.ProcessSplitParentWrite(&proto.WriteRequest{Puts: []*proto.PutRequest{{Key: "b", Value: []byte("0")}}},
		6, 4, 0, NoOpCallback)
	require.NoError(t, err)
	requireCommitEntry(t, db, 6, 4)
}

// A database that a binary before the term was recorded wrote has a commit
// offset and no term: the term reads as unknown until the next entry applies.
func TestDBReadsAnUnknownTermWithoutTheRecord(t *testing.T) {
	factory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	db, err := NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_NATURAL, 0, commontime.SystemClock)
	require.NoError(t, err)
	_, err = db.ProcessWrite(&proto.WriteRequest{Puts: []*proto.PutRequest{{Key: "a", Value: []byte("0")}}},
		1, 9, 0, NoOpCallback)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// What an older binary leaves: the commit offset alone
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
	require.NoError(t, err)
	batch := kv.NewWriteBatch()
	require.NoError(t, batch.Delete(commitTermKey))
	require.NoError(t, batch.Commit())
	require.NoError(t, batch.Close())
	require.NoError(t, kv.Close())

	db, err = NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_NATURAL, 0, commontime.SystemClock)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, db.Close())
		assert.NoError(t, factory.Close())
	})
	requireCommitEntry(t, db, wal.InvalidTerm, 9)

	_, err = db.ProcessWrite(&proto.WriteRequest{Puts: []*proto.PutRequest{{Key: "a", Value: []byte("0")}}},
		2, 10, 0, NoOpCallback)
	require.NoError(t, err)
	requireCommitEntry(t, db, 2, 10)
}

// The term is outside the replicas' checksum: a replica that records it and
// one that doesn't, on an older binary, keep the same checksum for the same
// entries. The same writes at different terms give the same checksum.
func TestDBCommitTermIsOutsideTheChecksum(t *testing.T) {
	checksum := func(term int64) uint32 {
		db := newCommitTermTestDB(t)
		db.EnableFeature(proto.Feature_FEATURE_DB_CHECKSUM)
		for offset := range int64(3) {
			_, err := db.ProcessWrite(&proto.WriteRequest{Puts: []*proto.PutRequest{{Key: "a", Value: []byte("0")}}},
				term, offset, 0, NoOpCallback)
			require.NoError(t, err)
		}
		return uint32(db.ReadChecksum())
	}

	assert.NotZero(t, checksum(1))
	assert.Equal(t, checksum(1), checksum(2))
}

// A checkpoint of the database carries the term of the last entry it
// applied: a replica that installs it as a snapshot reads that term.
func TestDBCheckpointCarriesTheCommitTerm(t *testing.T) {
	db := newCommitTermTestDB(t)
	for offset := range int64(3) {
		_, err := db.ProcessWrite(&proto.WriteRequest{Puts: []*proto.PutRequest{{Key: "a", Value: []byte("0")}}},
			4, offset, 0, NoOpCallback)
		require.NoError(t, err)
	}
	snapshot, err := db.Snapshot()
	require.NoError(t, err)
	defer func() { assert.NoError(t, snapshot.Close()) }()

	reader, err := kvstore.OpenSnapshotReader(snapshot.BasePath())
	require.NoError(t, err)
	defer func() { assert.NoError(t, reader.Close()) }()
	term, err := readASCIILongOrDefault(reader, commitTermKey, wal.InvalidTerm)
	require.NoError(t, err)
	assert.EqualValues(t, 4, term)
}

// The last applied entry can't be read when either of its records can't.
func TestDBReadCommitEntryIdFailsWhenARecordCantBeRead(t *testing.T) {
	for _, key := range []string{commitOffsetKey, commitTermKey} {
		t.Run(key, func(t *testing.T) {
			d, kv := newHookedTestDB(t)
			kv.failOn(opGet, key)

			_, err := d.ReadCommitEntryId()
			assert.ErrorIs(t, err, errInjected)
		})
	}
}
