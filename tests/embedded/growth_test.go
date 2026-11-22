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
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/failpoint"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia"
	"github.com/oxia-db/oxia/oxiad/common/dataserver"
	shardcontroller "github.com/oxia-db/oxia/oxiad/coordinator/controller/shard"
)

// Raising the replication factor of a namespace grows the ensemble of its
// shard one member at a time, each with a copy of the data, and never leads a
// term twice.
func TestGrowthRaisesTheReplicationFactor(t *testing.T) {
	c := newGrowingCluster(t)
	leaders := c.watchLeaders()
	keys := c.writeKeys("before", 200)

	c.raiseReplicationFactor(3)
	c.grownShard()

	keys = append(keys, c.writeKeys("after", 50)...)
	leaders.assertNoTermLedTwice()
	c.assertEveryServerHolds(keys)
}

// A coordinator that stops at any step of a growth leaves a shard that the
// next one grows to the replication factor: none of the steps is lost, or
// done twice.
func TestGrowthSurvivesACoordinatorStoppedAtEachStep(t *testing.T) {
	for _, step := range []string{
		"growth.afterCatchUp",
		"election.afterNewTerm",
		"election.afterFence",
		"election.afterLeaderStored",
		"election.afterBecomeLeader",
	} {
		t.Run(step, func(t *testing.T) {
			c := newGrowingCluster(t)
			leaders := c.watchLeaders()
			keys := c.writeKeys("before", 200)

			// The first growth stops at the step, as a coordinator that
			// stops there: nothing runs after it
			reached := make(chan struct{}, 1)
			failpoint.Enable(step, func(ctx context.Context) error {
				select {
				case reached <- struct{}{}:
				default:
				}
				<-ctx.Done()
				return ctx.Err()
			})
			t.Cleanup(func() { failpoint.Disable(step) })
			c.raiseReplicationFactor(3)
			select {
			case <-reached:
			case <-time.After(time.Minute):
				require.FailNow(t, "the growth never reached the step")
			}
			c.stopCoordinator()
			failpoint.Disable(step)
			c.startCoordinator()

			c.grownShard()
			keys = append(keys, c.writeKeys("after", 50)...)
			leaders.assertNoTermLedTwice()
			c.assertEveryServerHolds(keys)
		})
	}
}

// A new member that fails once its growth is recorded has the growth undone:
// the ensemble is back to the members before it, the new member's copy is to
// be deleted, and the shard grows onto another data server.
func TestGrowthIsUndoneWhenTheNewMemberFails(t *testing.T) {
	c := newGrowingCluster(t)
	leaders := c.watchLeaders()
	keys := c.writeKeys("before", 200)

	// The election that records the growth waits while the test stops the
	// new member
	recorded, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	failpoint.Enable("election.afterNewTerm", func(ctx context.Context) error {
		first := false
		once.Do(func() { first = true })
		if !first {
			return nil
		}
		close(recorded)
		select {
		case <-resume:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	t.Cleanup(func() { failpoint.Disable("election.afterNewTerm") })
	c.raiseReplicationFactor(3)
	select {
	case <-recorded:
	case <-time.After(time.Minute):
		require.FailNow(t, "the growth was never recorded")
	}
	change := c.status().GetEnsembleChange()
	require.NotNil(t, change)
	require.Nil(t, change.GetFrom())
	failed := c.index(change.GetTo())
	c.stopServer(failed)
	close(resume)

	shard := c.awaitShard(0, func(shard *proto.ShardMetadata) bool {
		return len(shard.GetEnsemble()) == 2 && shard.GetEnsembleChange() == nil &&
			!slices.ContainsFunc(shard.GetEnsemble(), dataserver.MatchDataServer(c.ids[failed])) &&
			slices.ContainsFunc(shard.GetPendingDeleteShardNodes(), dataserver.MatchDataServer(c.ids[failed])) &&
			shard.GetEnsembleCaughtUp()
	})
	t.Logf("the growth onto server %d was undone; the ensemble grew to %v instead", failed, shard.GetEnsemble())

	keys = append(keys, c.writeKeys("after", 50)...)
	leaders.assertNoTermLedTwice()
	c.assertMembersHold(shard, keys)
}

// newGrowingCluster starts the cluster with its namespace at replication
// factor 1, and a scheduler that runs a round every 100ms.
func newGrowingCluster(t *testing.T) *restartableCluster {
	t.Helper()
	c := newRestartableClusterWith(t, &proto.ClusterConfiguration{
		Namespaces:   []*proto.Namespace{{Name: constant.DefaultNamespace, ReplicationFactor: 1, InitialShardCount: 1}},
		LoadBalancer: &proto.LoadBalancer{ScheduleInterval: "100ms"},
	})
	shard := c.shard(0)
	require.Len(t, shard.GetEnsemble(), 1)
	return c
}

func (c *restartableCluster) admin() oxia.AdminClient {
	admin, err := oxia.NewAdminClient(c.adminAddr, nil, nil)
	require.NoError(c.t, err)
	c.t.Cleanup(func() { _ = admin.Close() })
	return admin
}

func (c *restartableCluster) raiseReplicationFactor(replicationFactor uint32) {
	_, err := c.admin().PatchNamespace(c.t.Context(),
		&proto.Namespace{Name: constant.DefaultNamespace, ReplicationFactor: replicationFactor})
	require.NoError(c.t, err)
}

// status is the status of the shard that the coordinator stores.
func (c *restartableCluster) status() *proto.ShardMetadata {
	return c.statusVia(c.admin())
}

// grownShard waits for the shard to be steady with its three members caught
// up, and nothing left pending: no change of the ensemble, and no copy to
// delete. A growth abandoned leaves the copy of its new member to the
// periodic deletion of the coordinator, every minute or so, before the shard
// grows onto it again.
func (c *restartableCluster) grownShard() *proto.ShardMetadata {
	return c.awaitShardWithin(0, func(shard *proto.ShardMetadata) bool {
		return len(shard.GetEnsemble()) == 3 && shard.GetEnsembleChange() == nil &&
			len(shard.GetPendingDeleteShardNodes()) == 0 && shard.GetEnsembleCaughtUp()
	}, 3*time.Minute)
}

// writeKeys writes count keys named after prefix, and returns them once
// acknowledged.
func (c *restartableCluster) writeKeys(prefix string, count int) []string {
	client, err := oxia.NewSyncClient(c.shard(0).GetLeader().GetPublic())
	require.NoError(c.t, err)
	defer client.Close()
	keys := make([]string, count)
	for i := range keys {
		keys[i] = fmt.Sprintf("%s-%04d", prefix, i)
		ctx, cancel := context.WithTimeout(c.t.Context(), 10*time.Second)
		_, _, err := client.Put(ctx, keys[i], []byte(keys[i]))
		cancel()
		require.NoError(c.t, err)
	}
	return keys
}

// assertEveryServerHolds checks that every data server applied the keys.
func (c *restartableCluster) assertEveryServerHolds(keys []string) {
	c.assertMembersHold(c.shard(0), keys)
}

// assertMembersHold waits for every member of the shard to apply what its
// leader committed, stops the cluster, and checks that each holds the keys.
func (c *restartableCluster) assertMembersHold(shard *proto.ShardMetadata, keys []string) {
	leader := c.index(shard.GetLeader())
	var members []int
	for _, member := range shard.GetEnsemble() {
		i := c.index(member)
		members = append(members, i)
		if i != leader {
			c.awaitApplied(i, leader)
		}
	}
	c.stopAll()
	for _, i := range members {
		assert.Empty(c.t, c.missing(i, keys), "server %d misses keys", i)
	}
}

// leaderWatch records the leader of each term of the shard that the
// coordinator stores, polled every 20ms, until the test ends.
type leaderWatch struct {
	t       *testing.T
	mu      sync.Mutex
	leaders map[int64]string
	twice   []string
}

func (c *restartableCluster) watchLeaders() *leaderWatch {
	w := &leaderWatch{t: c.t, leaders: make(map[int64]string)}
	admin := c.admin()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	c.t.Cleanup(func() {
		cancel()
		wg.Wait()
	})
	wg.Go(func() {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			// The coordinator may be stopped: the next round tries again
			if namespace, err := admin.GetNamespace(ctx, constant.DefaultNamespace); err == nil {
				w.record(namespace.GetNamespaceStatus().GetShards()[0])
			}
			select {
			case <-ticker.C:
			case <-ctx.Done():
				return
			}
		}
	})
	return w
}

func (w *leaderWatch) record(shard *proto.ShardMetadata) {
	if shard.GetLeader() == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	leader := shard.GetLeader().GetPublic()
	if previous, ok := w.leaders[shard.GetTerm()]; ok && previous != leader {
		w.twice = append(w.twice, fmt.Sprintf("term %d: %s, then %s", shard.GetTerm(), previous, leader))
	}
	w.leaders[shard.GetTerm()] = leader
}

func (w *leaderWatch) assertNoTermLedTwice() {
	w.mu.Lock()
	defer w.mu.Unlock()
	assert.NotEmpty(w.t, w.leaders, "no leader was seen")
	assert.Empty(w.t, w.twice, "a term was led by two data servers")
}

// A growth seeds its new member with a snapshot: on an idle shard, the member
// then reports the commit offset of its database, with no term. When the
// data server of the leader is removed, the swap off it elects such a member,
// which holds every committed entry, instead of undoing itself for good; the
// member, with nothing in its WAL, seeds the next one by a snapshot in turn.
func TestGrowthSeededMembersTakeOverFromARemovedIdleLeader(t *testing.T) {
	c := newGrowingCluster(t)
	// More entries than a new member can lag behind: it catches up by the
	// snapshot before its growth is recorded
	keys := c.writeKeys("before", shardcontroller.GrowthCatchUpLag+100)

	c.raiseReplicationFactor(2)
	grown := c.awaitShardWithin(0, func(shard *proto.ShardMetadata) bool {
		return len(shard.GetEnsemble()) == 2 && shard.GetEnsembleChange() == nil &&
			len(shard.GetPendingDeleteShardNodes()) == 0 && shard.GetEnsembleCaughtUp()
	}, 3*time.Minute)

	// The shard stays idle from now on
	leader := grown.GetLeader()
	_, err := c.admin().DeleteDataServer(c.t.Context(), leader.GetNameOrDefault())
	require.NoError(t, err)
	moved := c.awaitShardWithin(0, func(shard *proto.ShardMetadata) bool {
		return len(shard.GetEnsemble()) == 2 && shard.GetEnsembleChange() == nil &&
			!slices.ContainsFunc(shard.GetEnsemble(), dataserver.MatchDataServer(leader)) && shard.GetEnsembleCaughtUp()
	}, 2*time.Minute)

	c.assertMembersHold(moved, keys)
}

// A removed data server that holds the only replica of a shard has nowhere
// to move it while the namespace's replication factor is 1: it stays
// draining, and the shard keeps it as its member, round after round of the
// scheduler, so nothing committed is lost.
func TestRemovedDataServerWithTheOnlyReplicaStaysDraining(t *testing.T) {
	c := newGrowingCluster(t)
	keys := c.writeKeys("before", 10)
	founder := c.shard(0).GetLeader()
	admin := c.admin()

	_, err := admin.DeleteDataServer(t.Context(), founder.GetNameOrDefault())
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return c.dataServerState(admin, founder) == proto.DataServerState_DATA_SERVER_STATE_DRAINING
	}, 10*time.Second, 10*time.Millisecond)

	// Thirty rounds of the scheduler
	require.Never(t, func() bool {
		shard := c.statusVia(admin)
		return c.dataServerState(admin, founder) != proto.DataServerState_DATA_SERVER_STATE_DRAINING ||
			len(shard.GetEnsemble()) != 1 || !dataserver.MatchDataServer(founder)(shard.GetEnsemble()[0]) ||
			shard.GetEnsembleChange() != nil
	}, 3*time.Second, 50*time.Millisecond, "the only replica left the removed data server")

	c.assertMembersHold(c.shard(0), keys)
}

// A move to other data servers removes the one that holds the only replica
// of a shard and raises the replication factor to the servers left, in one
// change of the configuration: the shard grows onto them, then swaps the
// removed server out, which drains. Its copy of the shard is deleted later,
// by the periodic deletion of the coordinator.
func TestRemovedDataServerWithTheOnlyReplicaDrainsOnceTheShardGrows(t *testing.T) {
	c := newGrowingCluster(t)
	keys := c.writeKeys("before", 10)
	founder := c.shard(0).GetLeader()
	admin := c.admin()

	_, err := admin.DeleteDataServer(t.Context(), founder.GetNameOrDefault())
	require.NoError(t, err)
	c.raiseReplicationFactor(2)

	moved := c.awaitShardWithin(0, func(shard *proto.ShardMetadata) bool {
		return len(shard.GetEnsemble()) == 2 && shard.GetEnsembleChange() == nil && shard.GetEnsembleCaughtUp() &&
			!slices.ContainsFunc(shard.GetEnsemble(), dataserver.MatchDataServer(founder))
	}, 3*time.Minute)
	require.Eventually(t, func() bool {
		return c.dataServerState(admin, founder) == proto.DataServerState_DATA_SERVER_STATE_DRAINED
	}, 30*time.Second, 10*time.Millisecond)

	c.assertMembersHold(moved, keys)
}

// statusVia is the status of the shard that the coordinator stores, read
// through admin.
func (c *restartableCluster) statusVia(admin oxia.AdminClient) *proto.ShardMetadata {
	namespace, err := admin.GetNamespace(c.t.Context(), constant.DefaultNamespace)
	require.NoError(c.t, err)
	return namespace.GetNamespaceStatus().GetShards()[0]
}

// dataServerState is the state the coordinator reports for the data server.
func (c *restartableCluster) dataServerState(admin oxia.AdminClient, identity *proto.DataServerIdentity) proto.DataServerState {
	view, err := admin.GetDataServer(c.t.Context(), identity.GetNameOrDefault())
	require.NoError(c.t, err)
	return view.GetDataServerStatus().GetState()
}
