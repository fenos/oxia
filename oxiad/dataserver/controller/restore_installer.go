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

package controller

import (
	"context"
	"io"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/common/proto"
	commontime "github.com/oxia-db/oxia/common/time"
	"github.com/oxia-db/oxia/oxiad/dataserver/database"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"
	"github.com/oxia-db/oxia/oxiad/dataserver/option"
	"github.com/oxia-db/oxia/oxiad/dataserver/wal"
)

// Installer installs the database of a shard from its files, for a restore
// from a backup: the files are written through the snapshot loader, which
// stages them beside the database of the shard until they replace it, and the
// database is then opened once to filter it to the hash range of the shard and
// read its commit offset and term. The next term of the shard opens it as
// usual.
type Installer interface {
	// AddFile writes a file of the database, of the given size.
	AddFile(name string, size int64, r io.Reader) error
	// Complete replaces the database of the shard with the files, unless they
	// don't open as a database, and clears its WAL, which the restored
	// database is ahead of. It then filters the database to the hash range,
	// and returns its commit offset and term.
	Complete(hashRange *proto.HashRange) (commitOffset int64, term int64, err error)
	// Close deletes the files of an installation that didn't complete.
	Close() error
}

type installer struct {
	storageOptions *option.StorageOptions
	namespace      string
	shardId        int64
	walFactory     wal.Factory
	kvFactory      kvstore.Factory
	loader         kvstore.SnapshotLoader
}

// newInstaller prepares the snapshot loader. A restore doesn't know the size
// of the database up front: the loader only checks the headroom it leaves on
// the disk.
func newInstaller(ctx context.Context, storageOptions *option.StorageOptions, namespace string, shardId int64,
	walFactory wal.Factory, kvFactory kvstore.Factory) (Installer, error) {
	loader, err := kvFactory.NewSnapshotLoader(ctx, namespace, shardId, 0)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create the snapshot loader")
	}
	return &installer{
		storageOptions: storageOptions,
		namespace:      namespace,
		shardId:        shardId,
		walFactory:     walFactory,
		kvFactory:      kvFactory,
		loader:         loader,
	}, nil
}

func (i *installer) AddFile(name string, size int64, r io.Reader) error {
	chunkCount := int32(max(1, (size+kvstore.MaxSnapshotChunkSize-1)/kvstore.MaxSnapshotChunkSize))
	buf := make([]byte, kvstore.MaxSnapshotChunkSize)
	remaining := size
	for index := range chunkCount {
		chunk := buf[:min(remaining, kvstore.MaxSnapshotChunkSize)]
		if _, err := io.ReadFull(r, chunk); err != nil {
			return errors.Wrapf(err, "failed to read %s: want %d bytes", name, size)
		}
		remaining -= int64(len(chunk))
		if err := i.loader.AddChunk(name, index, chunkCount, chunk); err != nil {
			return errors.Wrapf(err, "failed to write %s", name)
		}
	}
	if _, err := r.Read(buf[:1]); !errors.Is(err, io.EOF) {
		return errors.Errorf("%s is longer than its size %d", name, size)
	}
	return nil
}

func (i *installer) Complete(hashRange *proto.HashRange) (commitOffset int64, term int64, err error) {
	err = i.loader.Install(func(dir string) error {
		_, err := database.ReadCheckpoint(dir)
		return err
	}, func() error {
		return wal.ClearShard(i.walFactory, i.namespace, i.shardId)
	})
	if err != nil {
		return 0, 0, errors.Wrap(err, "failed to install the database")
	}
	db, err := database.NewDB(i.namespace, i.shardId, i.kvFactory, proto.KeySortingType_UNKNOWN,
		i.storageOptions.Notification.Retention.ToDuration(), commontime.SystemClock)
	if err != nil {
		return 0, 0, errors.Wrap(err, "failed to open the database")
	}
	commitOffset, term, err = filterRestoredDatabase(db, hashRange)
	return commitOffset, term, multierr.Append(err, db.Close())
}

// filterRestoredDatabase deletes the records outside the hash range: the
// backup may be of a shard that covered more, and a split child's may hold
// records of its parent.
func filterRestoredDatabase(db database.DB, hashRange *proto.HashRange) (commitOffset int64, term int64, err error) {
	if err := database.FilterDBForSplit(db.RawKV(), hashRange); err != nil {
		return 0, 0, errors.Wrap(err, "failed to filter the database")
	}
	if err := database.ClearSplitFilters(db.RawKV()); err != nil {
		return 0, 0, err
	}
	if commitOffset, err = db.ReadCommitOffset(); err != nil {
		return 0, 0, errors.Wrap(err, "failed to read the commit offset")
	}
	if term, _, err = db.ReadTerm(); err != nil {
		return 0, 0, errors.Wrap(err, "failed to read the term")
	}
	return commitOffset, term, nil
}

func (i *installer) Close() error {
	return i.loader.Close()
}
