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

package embedded

import (
	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/dataserver"
	"github.com/oxia-db/oxia/oxiad/dataserver/wal"
)

// A replica whose WAL is empty, as after a snapshot, holds in its database
// the writes a later term committed; a replica that missed them holds a longer
// WAL of an earlier term, whose last writes were never acknowledged. Their
// heads rank term first, as Raft ranks a snapshot by the term of its last
// entry: the first leads, starts the second over from a snapshot of it, and the
// shard keeps every acknowledged write and none of the others.
func TestSnapshotHeadLeadsOverALongerWalOfAnEarlierTerm(t *testing.T) {
	c := newRestartableCluster(t)
	shard := c.caughtUpShard(0)
	require.NoError(t, c.put(shard.GetLeader(), "before", 10*time.Second))

	// 1. F leads, and appends writes that its followers never get
	f := c.index(shard.GetLeader())
	others := c.others(f)
	for _, i := range others {
		c.stopServer(i)
	}
	stale := make([]string, 10)
	for i := range stale {
		stale[i] = fmt.Sprintf("stale-%02d", i)
		require.Error(t, c.put(c.ids[f], stale[i], 500*time.Millisecond), "the write must not be acknowledged")
	}
	c.stopServer(f)

	// 2. The others lead a later term, and acknowledge writes that F misses
	for _, i := range others {
		c.startServer(i)
	}
	shard = c.shard(shard.GetTerm() + 1)
	laterTerm := shard.GetTerm()
	s := c.index(shard.GetLeader())
	acked := []string{"acked-0", "acked-1", "acked-2"}
	for _, key := range acked {
		require.NoError(t, c.put(shard.GetLeader(), key, 10*time.Second))
	}
	t.Logf("1-2. server %d appended %d writes never acknowledged; server %d led term %d and acknowledged %d",
		f, len(stale), s, laterTerm, len(acked))

	// 3. S loses its WAL: its database holds the acknowledged writes, as a
	// replica seeded by a snapshot of the leader does
	c.stopAll()
	walFactory := wal.NewWalFactory(&wal.FactoryOptions{BaseWalDir: filepath.Join(c.dir, strconv.Itoa(s), "wal")})
	walObject, err := walFactory.NewWal(constant.DefaultNamespace, 0, nil)
	require.NoError(t, err)
	require.NoError(t, walObject.Clear())
	require.NoError(t, walObject.Close())
	require.NoError(t, walFactory.Close())

	// 4. S and F restart, without the third server: the election fences both
	c.startServer(s)
	c.startServer(f)
	c.startCoordinator()
	shard = c.awaitShardWithin(laterTerm+1, func(shard *proto.ShardMetadata) bool {
		return dataserver.MatchDataServer(c.ids[s])(shard.GetLeader())
	}, 2*time.Minute)
	require.NoError(t, c.put(shard.GetLeader(), "after", 10*time.Second))
	c.awaitApplied(f, s)
	t.Logf("4. server %d leads term %d", s, shard.GetTerm())

	c.stopAll()
	for _, i := range []int{s, f} {
		assert.Empty(t, c.missing(i, slices.Concat([]string{"before", "after"}, acked)),
			"server %d misses acknowledged writes", i)
		assert.Len(t, c.missing(i, stale), len(stale), "server %d keeps writes never acknowledged", i)
	}
}
