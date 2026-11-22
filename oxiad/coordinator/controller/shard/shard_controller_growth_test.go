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

package shard

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonobject "github.com/oxia-db/oxia/common/object"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/errs"
	"github.com/oxia-db/oxia/oxiad/common/feature"
	metadatacommon "github.com/oxia-db/oxia/oxiad/common/metadata"
	metadatacodec "github.com/oxia-db/oxia/oxiad/common/metadata/codec"
	"github.com/oxia-db/oxia/oxiad/coordinator/action"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"
	coordrpc "github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc/rpctest"
)

const growthShard int64 = 5

// newGrowthController starts a controller of a shard led by s1 at term 2, with
// an ensemble of s1 alone, unless shardMeta is given, and verified at its
// start.
func newGrowthController(t *testing.T, metadata coordmetadata.Metadata, rpc *rpctest.RpcProvider,
	shardMeta *proto.ShardMetadata, opts ...Option) Controller {
	t.Helper()
	storeTestShardMetadata(t, metadata, constant.DefaultNamespace, growthShard, namespaceConfig, shardMeta)
	sc := NewController(constant.DefaultNamespace, growthShard, namespaceConfig, shardMeta, metadata,
		feature.NoOpSupplier, nil, coordrpc.ControllerRPCProvider(rpc), DefaultPeriodicTasksInterval, opts...)
	t.Cleanup(func() { assert.NoError(t, sc.Close()) })
	return sc
}

// steadyShard is a shard led by s1 at term 2, whose ensemble caught up.
func steadyShard(s1 *proto.DataServerIdentity, ensemble ...*proto.DataServerIdentity) *proto.ShardMetadata {
	return &proto.ShardMetadata{Status: proto.ShardStatusSteadyState, Term: 2, Leader: s1,
		Ensemble: ensemble, EnsembleCaughtUp: true}
}

func newGrowthTestMetadata(t *testing.T) coordmetadata.Metadata {
	t.Helper()
	return newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""),
		&proto.ClusterConfiguration{})
}

func growShard(t *testing.T, sc Controller, to *proto.DataServerIdentity) error {
	t.Helper()
	ac := action.NewChangeEnsembleAction(growthShard, nil, to)
	sc.Dispatch(ac)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	_, err := ac.Wait(ctx)
	return err
}

// A growth fences the new member at the current term, has the leader stream
// the shard to it as an observer until it caught up, and only then records the
// growth with an election: the new member is a member, with nothing to delete.
func TestControllerGrowthCatchesUpThenElects(t *testing.T) {
	s1, to := testDataServer("s1"), testDataServer("to")
	metadata := newGrowthTestMetadata(t)
	rpc := rpctest.NewRpcProvider()
	rpc.GetNode(s1).GetStatusResponse(2, proto.ServingStatus_LEADER, 100, 100)
	sc := newGrowthController(t, metadata, rpc, steadyShard(s1, s1))

	rpc.GetNode(to).NewTermResponse(-1, -1, nil)
	rpc.GetNode(s1).AddFollowerResponse(nil)
	rpc.GetNode(s1).GetStatusResponse(2, proto.ServingStatus_LEADER, 100, 100)
	rpc.GetNode(to).GetStatusResponse(2, proto.ServingStatus_FOLLOWER, 100, 100)
	rpc.GetNode(s1).NewTermResponse(2, 100, nil)
	rpc.GetNode(to).NewTermResponse(2, 100, nil)
	rpc.GetNode(s1).BecomeLeaderResponse(nil)
	rpc.GetNode(to).BecomeLeaderResponse(nil)

	require.NoError(t, growShard(t, sc, to))
	rpc.GetNode(to).ExpectNewTermRequest(t, growthShard, 2, true)
	observer := rpc.GetNode(s1).ExpectAddFollowerRequest(t, growthShard, 2)
	assert.True(t, observer.GetObserver())
	assert.Equal(t, to.GetInternal(), observer.GetFollowerName())
	rpc.GetNode(s1).ExpectNewTermRequest(t, growthShard, 3, true)
	rpc.GetNode(to).ExpectNewTermRequest(t, growthShard, 3, true)

	stored := requireShardMetadata(t, metadata, constant.DefaultNamespace, growthShard)
	assert.EqualValues(t, 3, stored.GetTerm())
	assertShardEnsemble(t, stored.GetEnsemble(), s1, to)
	assert.Nil(t, stored.GetEnsembleChange())
	assert.Empty(t, stored.GetPendingDeleteShardNodes())
}

// The new member is compared with the commit offset of the leader at each
// round: reaching what the leader had committed when the catch-up started
// isn't enough while the leader writes ahead. A growth whose new member never
// gets close gives up at its deadline: no election, the observer removed, and
// its copy to delete.
func TestControllerGrowthGivesUpWhenTheNewMemberStaysBehind(t *testing.T) {
	s1, to := testDataServer("s1"), testDataServer("to")
	metadata := newGrowthTestMetadata(t)
	rpc := rpctest.NewRpcProvider()
	rpc.GetNode(s1).GetStatusResponse(2, proto.ServingStatus_LEADER, 5000, 5000)
	sc := newGrowthController(t, metadata, rpc, steadyShard(s1, s1), WithGrowthTimeouts(1500*time.Millisecond, 0))

	rpc.GetNode(to).NewTermResponse(-1, -1, nil)
	rpc.GetNode(s1).AddFollowerResponse(nil)
	// The new member reaches the first commit offset of the leader, which is
	// far ahead by then
	rpc.GetNode(s1).GetStatusResponse(2, proto.ServingStatus_LEADER, 5000, 5000)
	rpc.GetNode(to).GetStatusResponse(2, proto.ServingStatus_FOLLOWER, 0, 0)
	for range 3 {
		rpc.GetNode(s1).GetStatusResponse(2, proto.ServingStatus_LEADER, 100_000, 100_000)
		rpc.GetNode(to).GetStatusResponse(2, proto.ServingStatus_FOLLOWER, 5000, 5000)
	}
	rpc.GetNode(s1).EnqueueGetStatusError(errors.New("unreachable"))
	rpc.GetNode(s1).GetStatusResponse(2, proto.ServingStatus_LEADER, 100_000, 100_000)
	rpc.GetNode(to).EnqueueGetStatusError(errors.New("unreachable"))
	rpc.GetNode(s1).RemoveObserverResponse(nil)

	err := growShard(t, sc, to)
	assert.ErrorIs(t, err, errs.ErrUnavailable)
	assert.ErrorContains(t, err, "didn't catch up in time")
	removed := rpc.GetNode(s1).ExpectRemoveObserverRequest(t, growthShard, 2)
	assert.Equal(t, to.GetInternal(), removed.GetFollowerName())
	assert.Equal(t, growthShard, removed.GetTargetShard())
	rpc.GetNode(s1).ExpectNoMoreNewTermRequest(t)

	stored := requireShardMetadata(t, metadata, constant.DefaultNamespace, growthShard)
	assert.EqualValues(t, 2, stored.GetTerm())
	assertShardEnsemble(t, stored.GetEnsemble(), s1)
	assertShardEnsemble(t, stored.GetPendingDeleteShardNodes(), to)
}

// A growth gives up when the new member can't be fenced, can't be added as an
// observer, or when the leader of the term it followed is gone.
func TestControllerGrowthGivesUpOnAFailedCatchUp(t *testing.T) {
	for _, test := range []struct {
		name   string
		script func(rpc *rpctest.RpcProvider, s1, to *proto.DataServerIdentity)
		reason string
	}{
		{name: "fence", reason: "fence the new member", script: func(rpc *rpctest.RpcProvider, _, to *proto.DataServerIdentity) {
			rpc.GetNode(to).NewTermResponse(0, 0, errors.New("unreachable"))
		}},
		{name: "observer", reason: "add the new member as an observer", script: func(rpc *rpctest.RpcProvider, s1, to *proto.DataServerIdentity) {
			rpc.GetNode(to).NewTermResponse(-1, -1, nil)
			rpc.GetNode(s1).AddFollowerResponse(errors.New("refused"))
			rpc.GetNode(s1).RemoveObserverResponse(errors.New("unreachable"))
		}},
		{name: "leader changed", reason: "the leader of term 2 changed", script: func(rpc *rpctest.RpcProvider, s1, to *proto.DataServerIdentity) {
			rpc.GetNode(to).NewTermResponse(-1, -1, nil)
			rpc.GetNode(s1).AddFollowerResponse(nil)
			rpc.GetNode(s1).GetStatusResponse(3, proto.ServingStatus_FOLLOWER, 100, 100)
			rpc.GetNode(s1).RemoveObserverResponse(nil)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			s1, to := testDataServer("s1"), testDataServer("to")
			metadata := newGrowthTestMetadata(t)
			rpc := rpctest.NewRpcProvider()
			rpc.GetNode(s1).GetStatusResponse(2, proto.ServingStatus_LEADER, 100, 100)
			sc := newGrowthController(t, metadata, rpc, steadyShard(s1, s1))
			test.script(rpc, s1, to)

			err := growShard(t, sc, to)
			assert.ErrorContains(t, err, test.reason)
			stored := requireShardMetadata(t, metadata, constant.DefaultNamespace, growthShard)
			assertShardEnsemble(t, stored.GetEnsemble(), s1)
			assertShardEnsemble(t, stored.GetPendingDeleteShardNodes(), to)
		})
	}
}

// A shard elected while the new member caught up has a new leader, which the
// observer didn't follow: the growth gives up instead of running.
func TestControllerGrowthGivesUpWhenTheShardMovedMeanwhile(t *testing.T) {
	s1, to := testDataServer("s1"), testDataServer("to")
	metadata := newGrowthTestMetadata(t)
	rpc := rpctest.NewRpcProvider()
	rpc.GetNode(s1).GetStatusResponse(2, proto.ServingStatus_LEADER, 100, 100)
	sc := newGrowthController(t, metadata, rpc, steadyShard(s1, s1))
	rpc.GetNode(to).NewTermResponse(-1, -1, nil)
	rpc.GetNode(s1).AddFollowerResponse(nil)

	ac := action.NewChangeEnsembleAction(growthShard, nil, to)
	sc.Dispatch(ac)
	rpc.GetNode(s1).ExpectAddFollowerRequest(t, growthShard, 2)
	require.NoError(t, metadata.PatchShardStatus(constant.DefaultNamespace, growthShard,
		func(current *proto.ShardMetadata) (bool, error) {
			current.Term = 7
			return true, nil
		}))
	rpc.GetNode(s1).GetStatusResponse(2, proto.ServingStatus_LEADER, 100, 100)
	rpc.GetNode(to).GetStatusResponse(2, proto.ServingStatus_FOLLOWER, 100, 100)

	_, err := ac.Wait(t.Context())
	assert.ErrorContains(t, err, "moved past term 2")
	assertShardEnsemble(t, requireShardMetadata(t, metadata, constant.DefaultNamespace, growthShard).
		GetPendingDeleteShardNodes(), to)
}

// A change that adds a data server whose copy is still to delete is refused,
// a growth as a swap: a coordinator that read the status earlier could delete
// its new copy.
func TestControllerChangeRefusesATargetPendingDeletion(t *testing.T) {
	s1, s2, to := testDataServer("s1"), testDataServer("s2"), testDataServer("to")
	metadata := newGrowthTestMetadata(t)
	rpc := rpctest.NewRpcProvider()
	rpc.GetNode(s1).GetStatusResponse(2, proto.ServingStatus_LEADER, 100, 100)
	rpc.GetNode(s2).GetStatusResponse(2, proto.ServingStatus_FOLLOWER, 100, 100)
	shardMeta := steadyShard(s1, s1, s2)
	shardMeta.PendingDeleteShardNodes = []*proto.DataServerIdentity{to}
	sc := newGrowthController(t, metadata, rpc, shardMeta)

	for _, from := range []*proto.DataServerIdentity{nil, s2} {
		ac := action.NewChangeEnsembleAction(growthShard, from, to)
		sc.Dispatch(ac)
		_, err := ac.Wait(t.Context())
		assert.ErrorIs(t, err, errs.ErrFailedPrecondition)
		assert.ErrorContains(t, err, "still to be deleted")
	}
	rpc.GetNode(to).ExpectNoMoreNewTermRequest(t)
}

// A second change is refused while the new member of a growth catches up, and
// a controller closed meanwhile fails the growth.
func TestControllerGrowthIsOneChangeAtATime(t *testing.T) {
	s1, to, other := testDataServer("s1"), testDataServer("to"), testDataServer("other")
	metadata := newGrowthTestMetadata(t)
	rpc := rpctest.NewRpcProvider()
	rpc.GetNode(s1).GetStatusResponse(2, proto.ServingStatus_LEADER, 100, 100)
	sc := newGrowthController(t, metadata, rpc, steadyShard(s1, s1))
	rpc.GetNode(to).NewTermResponse(-1, -1, nil)
	rpc.GetNode(s1).AddFollowerResponse(nil)

	growth := action.NewChangeEnsembleAction(growthShard, nil, to)
	sc.Dispatch(growth)
	rpc.GetNode(s1).ExpectAddFollowerRequest(t, growthShard, 2)

	second := action.NewChangeEnsembleAction(growthShard, nil, other)
	sc.Dispatch(second)
	_, err := second.Wait(t.Context())
	assert.ErrorContains(t, err, "is catching up")

	require.NoError(t, sc.Close())
	_, err = growth.Wait(t.Context())
	assert.Error(t, err)
}

// A shard without a leader can't grow.
func TestControllerGrowthNeedsALeader(t *testing.T) {
	s1, to := testDataServer("s1"), testDataServer("to")
	metadata := newGrowthTestMetadata(t)
	rpc := rpctest.NewRpcProvider()
	rpc.GetNode(s1).GetStatusResponse(2, proto.ServingStatus_LEADER, 100, 100)
	sc := newGrowthController(t, metadata, rpc, steadyShard(s1, s1)).(*controller)

	ac := action.NewChangeEnsembleAction(growthShard, nil, to)
	sc.startGrowth(&proto.ShardMetadata{Status: proto.ShardStatusElection, Term: 3}, ac)
	_, err := ac.Wait(t.Context())
	assert.ErrorContains(t, err, "no leader to grow its ensemble from")
}

// A controller that takes over a pending growth arms its deadline: a new member
// that stays behind past it has the growth undone, and the new member's copy
// deleted.
func TestControllerTakingOverAPendingGrowthUndoesItAtItsDeadline(t *testing.T) {
	s1, to := testDataServer("s1"), testDataServer("to")
	metadata := newGrowthTestMetadata(t)
	rpc := rpctest.NewRpcProvider()
	// The election that a pending change starts with
	rpc.GetNode(s1).NewTermResponse(2, 10, nil)
	rpc.GetNode(to).NewTermResponse(2, 5, nil)
	rpc.GetNode(s1).BecomeLeaderResponse(nil)
	// The one at the deadline
	rpc.GetNode(s1).NewTermResponse(3, 12, nil)
	rpc.GetNode(to).NewTermResponse(3, 6, nil)
	rpc.GetNode(s1).BecomeLeaderResponse(nil)
	shardMeta := steadyShard(s1, s1, to)
	shardMeta.EnsembleChange = &proto.EnsembleChangeMetadata{To: to}
	newGrowthController(t, metadata, rpc, shardMeta, WithGrowthTimeouts(0, 500*time.Millisecond))

	rpc.GetNode(s1).ExpectBecomeLeaderRequest(t, growthShard, 3, 2)
	rpc.GetNode(s1).ExpectBecomeLeaderRequest(t, growthShard, 4, 1)
	require.Eventually(t, func() bool {
		stored := requireShardMetadata(t, metadata, constant.DefaultNamespace, growthShard)
		return stored.GetEnsembleChange() == nil && len(stored.GetEnsemble()) == 1
	}, 10*time.Second, 10*time.Millisecond)
	stored := requireShardMetadata(t, metadata, constant.DefaultNamespace, growthShard)
	assertShardEnsemble(t, stored.GetEnsemble(), s1)
	assertShardEnsemble(t, stored.GetPendingDeleteShardNodes(), to)
}

// A new member that fails while its growth is pending has the growth settled by
// an election: at two members, without it, the ensemble has no majority, so the
// growth is undone.
func TestControllerGrowthIsUndoneWhenTheNewMemberFails(t *testing.T) {
	s1, to := testDataServer("s1"), testDataServer("to")
	metadata := newGrowthTestMetadata(t)
	rpc := rpctest.NewRpcProvider()
	rpc.GetNode(s1).NewTermResponse(2, 10, nil)
	rpc.GetNode(to).NewTermResponse(2, 5, nil)
	rpc.GetNode(s1).BecomeLeaderResponse(nil)
	shardMeta := steadyShard(s1, s1, to)
	shardMeta.EnsembleChange = &proto.EnsembleChangeMetadata{To: to}
	sc := newGrowthController(t, metadata, rpc, shardMeta)
	rpc.GetNode(s1).ExpectBecomeLeaderRequest(t, growthShard, 3, 2)

	rpc.GetNode(s1).NewTermResponse(3, 12, nil)
	rpc.GetNode(to).NewTermResponse(0, 0, errors.New("unreachable"))
	rpc.GetNode(s1).BecomeLeaderResponse(nil)
	sc.BecameUnavailable(to)

	rpc.GetNode(s1).ExpectBecomeLeaderRequest(t, growthShard, 4, 1)
	require.Eventually(t, func() bool {
		return requireShardMetadata(t, metadata, constant.DefaultNamespace, growthShard).GetEnsembleChange() == nil
	}, 10*time.Second, 10*time.Millisecond)
	assertShardEnsemble(t, requireShardMetadata(t, metadata, constant.DefaultNamespace, growthShard).GetEnsemble(), s1)
}

// A swap finished by the tick of its election has its source deleted at the
// term of the election, without waiting for another one, and the shard has
// nothing left that holds a split back.
func TestControllerSwapFinishedByTheTickDeletesItsSource(t *testing.T) {
	s1, s2, s3 := testDataServer("s1"), testDataServer("s2"), testDataServer("s3")
	metadata := newGrowthTestMetadata(t)
	rpc := rpctest.NewRpcProvider()
	rpc.GetNode(s1).GetStatusResponse(2, proto.ServingStatus_LEADER, 10, 10)
	rpc.GetNode(s2).GetStatusResponse(2, proto.ServingStatus_FOLLOWER, 10, 10)
	storeTestShardMetadata(t, metadata, constant.DefaultNamespace, growthShard, namespaceConfig, steadyShard(s1, s1, s2))
	sc := NewController(constant.DefaultNamespace, growthShard, namespaceConfig, steadyShard(s1, s1, s2), metadata,
		feature.NoOpSupplier, nil, rpc, 100*time.Millisecond)
	t.Cleanup(func() { assert.NoError(t, sc.Close()) })

	rpc.GetNode(s1).NewTermResponse(2, 10, nil)
	rpc.GetNode(s2).NewTermResponse(2, 10, nil)
	rpc.GetNode(s3).NewTermResponse(-1, -1, nil)
	rpc.GetNode(s1).BecomeLeaderResponse(nil)
	rpc.GetNode(s3).GetStatusResponse(3, proto.ServingStatus_FOLLOWER, 10, 10)
	rpc.GetNode(s2).DeleteShardResponse(nil)

	ac := action.NewChangeEnsembleAction(growthShard, s2, s3)
	sc.Dispatch(ac)
	_, err := ac.Wait(t.Context())
	require.NoError(t, err)

	rpc.GetNode(s2).ExpectDeleteShardRequest(t, growthShard, 3)
	require.Eventually(t, func() bool {
		stored := requireShardMetadata(t, metadata, constant.DefaultNamespace, growthShard)
		return stored.GetEnsembleChange() == nil && len(stored.GetPendingDeleteShardNodes()) == 0
	}, 10*time.Second, 10*time.Millisecond)
	stored := requireShardMetadata(t, metadata, constant.DefaultNamespace, growthShard)
	assert.EqualValues(t, 3, stored.GetTerm(), "no other election")
	assertShardEnsemble(t, stored.GetEnsemble(), s1, s3)
}

// The deadline of a growth that finished, or was replaced by another, does
// nothing; one that can't read the status tries again, and one whose shard
// was removed stops the controller.
func TestControllerGrowthDeadlineOfAnotherGrowthDoesNothing(t *testing.T) {
	s1, to := testDataServer("s1"), testDataServer("to")
	base := newGrowthTestMetadata(t)
	var failLoads atomic.Bool
	failed := make(chan struct{}, 1)
	metadata := &hookedMetadata{Metadata: base}
	metadata.getShardStatus = func(namespace string, shard int64) (commonobject.Borrowed[*proto.ShardMetadata], bool, error) {
		if failLoads.Load() {
			select {
			case failed <- struct{}{}:
			default:
			}
			return commonobject.Borrowed[*proto.ShardMetadata]{}, false, errors.New("store unavailable")
		}
		return base.GetShardStatus(namespace, shard)
	}
	rpc := rpctest.NewRpcProvider()
	rpc.GetNode(s1).GetStatusResponse(2, proto.ServingStatus_LEADER, 100, 100)
	storeTestShardMetadata(t, base, constant.DefaultNamespace, growthShard, namespaceConfig, steadyShard(s1, s1))
	sc := NewController(constant.DefaultNamespace, growthShard, namespaceConfig, steadyShard(s1, s1), metadata,
		feature.NoOpSupplier, nil, rpc, DefaultPeriodicTasksInterval).(*controller)
	t.Cleanup(func() { assert.NoError(t, sc.Close()) })
	rpc.GetNode(s1).ExpectGetStatusRequest(t, growthShard)

	sc.growthDeadlineOp <- to
	rpc.GetNode(s1).ExpectNoMoreNewTermRequest(t)

	failLoads.Store(true)
	sc.growthDeadlineOp <- to
	<-failed
	<-failed
	failLoads.Store(false)

	require.NoError(t, base.DeleteShardStatus(constant.DefaultNamespace, growthShard))
	sc.growthDeadlineOp <- to
	select {
	case <-sc.ctx.Done():
	case <-time.After(10 * time.Second):
		require.FailNow(t, "the controller of a removed shard kept running")
	}
}

// A growth that gives up after another coordinator recorded it, with the new
// member in the ensemble, leaves the member's copy alone; a failure to record
// a copy to delete is only logged.
func TestControllerGrowthGivingUpKeepsAMembersCopy(t *testing.T) {
	for _, test := range []struct {
		name   string
		record func(current *proto.ShardMetadata, to *proto.DataServerIdentity)
		fail   bool
	}{
		{name: "recorded by another coordinator", record: func(current *proto.ShardMetadata, to *proto.DataServerIdentity) {
			current.Term = 3
			current.Ensemble = append(current.Ensemble, to)
		}},
		{name: "failed to record the copy", fail: true, record: func(current *proto.ShardMetadata, _ *proto.DataServerIdentity) {
			current.Term = 3
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			s1, to := testDataServer("s1"), testDataServer("to")
			base := newGrowthTestMetadata(t)
			var failPatches atomic.Bool
			metadata := &hookedMetadata{Metadata: base}
			metadata.patchShardStatus = func(namespace string, shard int64, modify func(*proto.ShardMetadata) (bool, error)) error {
				if failPatches.Load() {
					return errors.New("store unavailable")
				}
				return base.PatchShardStatus(namespace, shard, modify)
			}
			rpc := rpctest.NewRpcProvider()
			rpc.GetNode(s1).GetStatusResponse(2, proto.ServingStatus_LEADER, 100, 100)
			storeTestShardMetadata(t, base, constant.DefaultNamespace, growthShard, namespaceConfig, steadyShard(s1, s1))
			sc := NewController(constant.DefaultNamespace, growthShard, namespaceConfig, steadyShard(s1, s1), metadata,
				feature.NoOpSupplier, nil, rpc, DefaultPeriodicTasksInterval)
			t.Cleanup(func() { assert.NoError(t, sc.Close()) })
			rpc.GetNode(to).NewTermResponse(-1, -1, nil)
			rpc.GetNode(s1).AddFollowerResponse(nil)

			ac := action.NewChangeEnsembleAction(growthShard, nil, to)
			sc.Dispatch(ac)
			rpc.GetNode(s1).ExpectAddFollowerRequest(t, growthShard, 2)
			require.NoError(t, base.PatchShardStatus(constant.DefaultNamespace, growthShard,
				func(current *proto.ShardMetadata) (bool, error) {
					test.record(current, to)
					return true, nil
				}))
			failPatches.Store(test.fail)
			// The leader moved on: the catch-up gives up
			rpc.GetNode(s1).GetStatusResponse(3, proto.ServingStatus_LEADER, 100, 100)
			rpc.GetNode(s1).RemoveObserverResponse(nil)

			_, err := ac.Wait(t.Context())
			assert.ErrorContains(t, err, "changed")
			failPatches.Store(false)
			assert.Empty(t, requireShardMetadata(t, base, constant.DefaultNamespace, growthShard).GetPendingDeleteShardNodes())
		})
	}
}

// A growth whose namespace options can't be read fails at once.
func TestControllerGrowthFailsWithoutTheTermOptions(t *testing.T) {
	s1, to := testDataServer("s1"), testDataServer("to")
	base := newGrowthTestMetadata(t)
	metadata := &hookedMetadata{Metadata: base}
	metadata.getNamespaceTermOptions = func(string) (*proto.NewTermOptions, error) {
		return nil, errors.New("store unavailable")
	}
	rpc := rpctest.NewRpcProvider()
	rpc.GetNode(s1).GetStatusResponse(2, proto.ServingStatus_LEADER, 100, 100)
	storeTestShardMetadata(t, base, constant.DefaultNamespace, growthShard, namespaceConfig, steadyShard(s1, s1))
	sc := NewController(constant.DefaultNamespace, growthShard, namespaceConfig, steadyShard(s1, s1), metadata,
		feature.NoOpSupplier, nil, rpc, DefaultPeriodicTasksInterval)
	t.Cleanup(func() { assert.NoError(t, sc.Close()) })

	assert.ErrorContains(t, growShard(t, sc, to), "store unavailable")
	rpc.GetNode(to).ExpectNoMoreNewTermRequest(t)
}

// An election that keeps a growth pending keeps its deadline armed, and the
// failure of a data server that isn't the new member of a growth elects
// nothing; an election whose status can't be read back arms no deadline.
func TestControllerGrowthDeadlineIsArmedOnce(t *testing.T) {
	s1, to, other := testDataServer("s1"), testDataServer("to"), testDataServer("other")
	base := newGrowthTestMetadata(t)
	var failNextLoad atomic.Bool
	failed := make(chan struct{}, 1)
	metadata := &hookedMetadata{Metadata: base}
	metadata.getShardStatus = func(namespace string, shard int64) (commonobject.Borrowed[*proto.ShardMetadata], bool, error) {
		if failNextLoad.CompareAndSwap(true, false) {
			failed <- struct{}{}
			return commonobject.Borrowed[*proto.ShardMetadata]{}, false, errors.New("store unavailable")
		}
		return base.GetShardStatus(namespace, shard)
	}
	rpc := rpctest.NewRpcProvider()
	shardMeta := steadyShard(s1, s1, to)
	shardMeta.EnsembleChange = &proto.EnsembleChangeMetadata{To: to}
	storeTestShardMetadata(t, base, constant.DefaultNamespace, growthShard, namespaceConfig, shardMeta)
	rpc.GetNode(s1).NewTermResponse(2, 10, nil)
	rpc.GetNode(to).NewTermResponse(2, 5, nil)
	sc := NewController(constant.DefaultNamespace, growthShard, namespaceConfig, shardMeta, metadata,
		feature.NoOpSupplier, nil, rpc, DefaultPeriodicTasksInterval).(*controller)
	t.Cleanup(func() { assert.NoError(t, sc.Close()) })

	// The status can't be read back once elected: no deadline yet
	rpc.GetNode(s1).ExpectNewTermRequest(t, growthShard, 3, true)
	failNextLoad.Store(true)
	rpc.GetNode(s1).BecomeLeaderResponse(nil)
	rpc.GetNode(s1).ExpectBecomeLeaderRequest(t, growthShard, 3, 2)
	<-failed

	// The leader fails: the next election keeps the growth pending, and arms
	// the deadline, which the one after keeps
	rpc.GetNode(s1).NewTermResponse(3, 10, nil)
	rpc.GetNode(to).NewTermResponse(3, 5, nil)
	rpc.GetNode(s1).BecomeLeaderResponse(nil)
	sc.BecameUnavailable(s1)
	rpc.GetNode(s1).ExpectNewTermRequest(t, growthShard, 4, true)
	rpc.GetNode(s1).ExpectBecomeLeaderRequest(t, growthShard, 4, 2)
	rpc.GetNode(s1).NewTermResponse(4, 10, nil)
	rpc.GetNode(to).NewTermResponse(4, 5, nil)
	rpc.GetNode(s1).BecomeLeaderResponse(nil)
	sc.BecameUnavailable(s1)
	rpc.GetNode(s1).ExpectNewTermRequest(t, growthShard, 5, true)
	rpc.GetNode(s1).ExpectBecomeLeaderRequest(t, growthShard, 5, 2)

	sc.BecameUnavailable(other)
	rpc.GetNode(s1).ExpectNoMoreNewTermRequest(t)
	require.NotNil(t, requireShardMetadata(t, base, constant.DefaultNamespace, growthShard).GetEnsembleChange())
}
