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
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia"
	"github.com/oxia-db/oxia/oxiad/common/errs"
	metadatacodec "github.com/oxia-db/oxia/oxiad/common/metadata/codec"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	"github.com/oxia-db/oxia/oxiad/coordinator"
	coordinatoroption "github.com/oxia-db/oxia/oxiad/coordinator/option"
	"github.com/oxia-db/oxia/oxiad/dataserver"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"
	dataserveroption "github.com/oxia-db/oxia/oxiad/dataserver/option"
	"github.com/oxia-db/oxia/tests/mock"
)

// The coordinator status restored from an older copy, while the data servers
// keep their state: the elections start from a term the servers are past.

// With the data servers 9 terms ahead of the status, the election catches up
// one term per attempt, a second apart.
func TestElectionFromStaleStatusCatchesUpOneTermPerAttempt(t *testing.T) {
	c := newRestartableCluster(t)
	shard := c.caughtUpShard(0)
	require.NoError(t, c.put(shard.GetLeader(), "key", 10*time.Second))
	stale := c.readStatus()
	staleTerm := shard.GetTerm()

	c.stopCoordinator()
	const serversTerm = 10
	for i := range c.servers {
		leader, err := c.servers[i].GetShardDirector().GetOrCreateLeader(constant.DefaultNamespace, 0, nil)
		require.NoError(t, err)
		_, err = leader.NewTerm(&proto.NewTermRequest{Namespace: constant.DefaultNamespace, Shard: 0, Term: serversTerm})
		require.NoError(t, err)
	}
	c.writeStatus(stale)

	start := time.Now()
	c.startCoordinator()
	shard = c.shard(staleTerm + 1)
	elapsed := time.Since(start)
	t.Logf("status at term %d, servers at term %d: led again at term %d after %v",
		staleTerm, serversTerm, shard.GetTerm(), elapsed)
	assert.Less(t, elapsed, 3*time.Second, "the election catches up one term per attempt")
}

// A follower is elected under a term that an earlier leader already used, and
// that leader later joins without being truncated: it keeps a write that was
// never acknowledged, and misses one that was.
func TestElectionFromStaleStatusLeadsTwiceUnderOneTerm(t *testing.T) {
	c := newRestartableCluster(t)
	shard := c.caughtUpShard(0)
	require.NoError(t, c.put(shard.GetLeader(), "key-0", 10*time.Second))
	stale := c.readStatus()
	staleTerm := shard.GetTerm()

	// 1. Two failovers, each writing a key
	for round := 1; round <= 2; round++ {
		leader := c.index(shard.GetLeader())
		c.stopServer(leader)
		shard = c.shard(shard.GetTerm() + 1)
		require.NoError(t, c.put(shard.GetLeader(), fmt.Sprintf("key-%d", round), 10*time.Second))
		c.startServer(leader)
		shard = c.caughtUpShard(shard.GetTerm())
	}
	usedTerm := shard.GetTerm()
	s1 := c.index(shard.GetLeader())
	followers := c.others(s1)
	t.Logf("1. status saved at term %d; term %d led by server %d", staleTerm, usedTerm, s1)

	// 2. S1 appends a write that its followers never get
	for _, i := range followers {
		c.stopServer(i)
	}
	require.Error(t, c.put(c.ids[s1], "lost", 2*time.Second), "the write must not be acknowledged")

	// 3. S1 restarts alone: the elections can't reach a quorum, and fence it
	// past the term it led
	c.stopServer(s1)
	c.startServer(s1)
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		assert.Greater(ct, c.term(s1), usedTerm)
	}, time.Minute, 50*time.Millisecond)
	t.Logf("3. server %d fenced at term %d", s1, c.term(s1))

	// 4. The status goes back to the older copy
	c.stopCoordinator()
	c.writeStatus(stale)
	for _, i := range followers {
		c.startServer(i)
	}
	c.startCoordinator()
	shard = c.shard(staleTerm + 1)
	reusedTerm := shard.GetTerm()
	leader := c.index(shard.GetLeader())
	t.Logf("4. server %d leads at term %d, which server %d already led", leader, reusedTerm, s1)
	require.NoError(t, c.put(shard.GetLeader(), "acked", 10*time.Second))
	require.NoError(t, c.put(shard.GetLeader(), "acked-2", 10*time.Second))

	// 5. The leader fails over: the next elections reach S1's term, and S1
	// joins
	c.stopServer(leader)
	shard = c.shard(reusedTerm + 1)
	leader = c.index(shard.GetLeader())
	t.Logf("5. server %d leads at term %d", leader, shard.GetTerm())
	require.NoError(t, c.put(shard.GetLeader(), "after", 10*time.Second))
	for _, i := range c.others(leader) {
		if c.servers[i] != nil {
			c.awaitApplied(i, leader)
		}
	}

	c.stopAll()
	for i := range c.ids {
		t.Logf("server %d: acked=%v acked-2=%v lost=%v after=%v",
			i, c.has(i, "acked"), c.has(i, "acked-2"), c.has(i, "lost"), c.has(i, "after"))
	}
	assert.Greater(t, reusedTerm, usedTerm, "term %d was led twice", usedTerm)
	for i := range c.ids {
		assert.False(t, c.has(i, "lost"), "server %d applied a write that was never acknowledged", i)
		assert.False(t, c.has(i, "after") && !c.has(i, "acked"),
			"server %d applied the writes after an acknowledged write, but not that write", i)
	}
}

// restartableCluster is three data servers and a coordinator on the file
// metadata provider, with one shard at replication factor 3. Each server
// restarts on its own addresses and directories.
type restartableCluster struct {
	t         *testing.T
	dir       string
	ids       []*proto.DataServerIdentity
	servers   []*dataserver.Server
	coord     coordinator.Coordinator
	adminAddr string
}

func newRestartableCluster(t *testing.T) *restartableCluster {
	t.Helper()
	c := &restartableCluster{t: t, dir: t.TempDir(), ids: newDataServerIdentities(t),
		servers: make([]*dataserver.Server, 3), adminAddr: mock.FreeAddresses(t, 1)[0]}
	for i := range c.ids {
		c.startServer(i)
	}
	options := c.coordinatorOptions()
	config, err := metadatacodec.ClusterConfigCodec.MarshalYAML(&proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{{Name: constant.DefaultNamespace, ReplicationFactor: 3, InitialShardCount: 1}},
		Servers:    c.ids,
	})
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(options.Metadata.File.Dir, 0o755))
	require.NoError(t, os.WriteFile(options.Metadata.File.ConfigPath(), config, 0o600))
	c.startCoordinator()
	t.Cleanup(c.stopAll)
	return c
}

func (c *restartableCluster) dbDir(i int) string { return filepath.Join(c.dir, strconv.Itoa(i), "db") }

func (c *restartableCluster) startServer(i int) {
	options := dataserveroption.NewDefaultOptions()
	options.Server.Public.BindAddress = c.ids[i].Public
	options.Server.Internal.BindAddress = c.ids[i].Internal
	options.Observability.Metric.Enabled = &constant.FlagFalse
	options.Storage.Database.Dir = c.dbDir(i)
	options.Storage.WAL.Dir = filepath.Join(c.dir, strconv.Itoa(i), "wal")
	server, err := dataserver.New(c.t.Context(), options)
	require.NoError(c.t, err)
	c.servers[i] = server
}

func (c *restartableCluster) stopServer(i int) {
	require.NoError(c.t, c.servers[i].Close())
	c.servers[i] = nil
}

func (c *restartableCluster) coordinatorOptions() *coordinatoroption.Options {
	options := coordinatoroption.NewDefaultOptions()
	options.Server.Public.BindAddress = c.adminAddr
	options.Server.Internal.BindAddress = "localhost:0"
	options.Observability.Metric.Enabled = &constant.FlagFalse
	options.Metadata.ProviderName = coordinatoroption.ProviderFile
	options.Metadata.File.Dir = filepath.Join(c.dir, "coordinator")
	return options
}

func (c *restartableCluster) startCoordinator() {
	coord, err := coordinator.New(c.t.Context(), commonwatch.New(c.coordinatorOptions()))
	require.NoError(c.t, err)
	c.coord = coord
}

func (c *restartableCluster) stopCoordinator() {
	require.NoError(c.t, c.coord.Close())
	c.coord = nil
}

func (c *restartableCluster) stopAll() {
	if c.coord != nil {
		c.stopCoordinator()
	}
	for i, server := range c.servers {
		if server != nil {
			c.stopServer(i)
		}
	}
}

func (c *restartableCluster) readStatus() []byte {
	status, err := os.ReadFile(c.coordinatorOptions().Metadata.File.StatusPath())
	require.NoError(c.t, err)
	return status
}

func (c *restartableCluster) writeStatus(status []byte) {
	require.NoError(c.t, os.WriteFile(c.coordinatorOptions().Metadata.File.StatusPath(), status, 0o600))
}

func (c *restartableCluster) index(identity *proto.DataServerIdentity) int {
	for i, id := range c.ids {
		if id.Public == identity.GetPublic() {
			return i
		}
	}
	require.FailNow(c.t, "unknown data server", "%v", identity)
	return -1
}

func (c *restartableCluster) others(i int) []int {
	var others []int
	for j := range c.ids {
		if j != i {
			others = append(others, j)
		}
	}
	return others
}

// shard waits for the shard to be steady with a leader at minTerm or above.
func (c *restartableCluster) shard(minTerm int64) *proto.ShardMetadata {
	return c.awaitShard(minTerm, func(*proto.ShardMetadata) bool { return true })
}

// caughtUpShard waits for the shard to be steady with a leader at minTerm or
// above, and with its whole ensemble caught up.
func (c *restartableCluster) caughtUpShard(minTerm int64) *proto.ShardMetadata {
	return c.awaitShard(minTerm, (*proto.ShardMetadata).GetEnsembleCaughtUp)
}

func (c *restartableCluster) awaitShard(minTerm int64, ready func(*proto.ShardMetadata) bool) *proto.ShardMetadata {
	admin, err := oxia.NewAdminClient(c.adminAddr, nil, nil)
	require.NoError(c.t, err)
	defer admin.Close()
	var shard *proto.ShardMetadata
	require.EventuallyWithT(c.t, func(t *assert.CollectT) {
		namespace, err := admin.GetNamespace(c.t.Context(), constant.DefaultNamespace)
		if !assert.NoError(t, err) {
			return
		}
		shard = namespace.GetNamespaceStatus().GetShards()[0]
		assert.Equal(t, proto.ShardStatusSteadyState, shard.GetStatusOrDefault())
		assert.NotNil(t, shard.GetLeader())
		assert.GreaterOrEqual(t, shard.GetTerm(), minTerm)
		assert.True(t, ready(shard))
	}, time.Minute, 50*time.Millisecond)
	return shard
}

func (c *restartableCluster) put(via *proto.DataServerIdentity, key string, timeout time.Duration) error {
	client, err := oxia.NewSyncClient(via.GetPublic())
	require.NoError(c.t, err)
	defer client.Close()
	ctx, cancel := context.WithTimeout(c.t.Context(), timeout)
	defer cancel()
	_, _, err = client.Put(ctx, key, []byte(key))
	return err
}

// term is the term of the shard on server i, held by its leader or its
// follower controller, or 0.
func (c *restartableCluster) term(i int) int64 {
	director := c.servers[i].GetShardDirector()
	if leader, err := director.GetLeader(0); err == nil {
		if status, err := leader.GetStatus(&proto.GetStatusRequest{Shard: 0}); err == nil {
			return status.GetTerm()
		}
	}
	if follower, err := director.GetFollower(0); err == nil {
		return follower.Term()
	}
	return 0
}

// awaitApplied waits until server i applied every entry that the leader on
// server leader committed.
func (c *restartableCluster) awaitApplied(i, leader int) {
	require.EventuallyWithT(c.t, func(t *assert.CollectT) {
		lc, err := c.servers[leader].GetShardDirector().GetLeader(0)
		if !assert.NoError(t, err) {
			return
		}
		status, err := lc.GetStatus(&proto.GetStatusRequest{Shard: 0})
		if !assert.NoError(t, err) {
			return
		}
		follower, err := c.servers[i].GetShardDirector().GetFollower(0)
		if !assert.NoError(t, err) {
			return
		}
		assert.GreaterOrEqual(t, follower.CommitOffset(), status.GetCommitOffset())
	}, time.Minute, 50*time.Millisecond)
}

// has reports whether the database of server i, which must be stopped, holds
// key.
func (c *restartableCluster) has(i int, key string) bool {
	factory, err := kvstore.NewPebbleKVFactory(&kvstore.FactoryOptions{DataDir: c.dbDir(i), CacheSizeMB: 1, UseWAL: true})
	require.NoError(c.t, err)
	defer factory.Close()
	kv, err := factory.NewKV(constant.DefaultNamespace, 0, proto.KeySortingType_HIERARCHICAL)
	require.NoError(c.t, err)
	defer kv.Close()
	_, _, closer, err := kv.Get(key, kvstore.ComparisonEqual, kvstore.NoInternalKeys)
	if errors.Is(err, errs.ErrDBKeyNotFound) {
		return false
	}
	require.NoError(c.t, err)
	require.NoError(c.t, closer.Close())
	return true
}
