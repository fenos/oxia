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

package raft

import (
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	metadatacodec "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common/codec"
)

func freeAddresses(t *testing.T, n int) []string {
	t.Helper()

	addrs := make([]string, n)
	for i := range addrs {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		addrs[i] = listener.Addr().String()
		require.NoError(t, listener.Close())
	}
	return addrs
}

// startPeers brings up one raft node per address, every one configured
// with the same peer list, and returns them in list order.
func startPeers(t *testing.T, peers []string, promotionQuiet time.Duration) []*Raft {
	t.Helper()

	nodes := make([]*Raft, len(peers))
	for i, addr := range peers {
		node, err := New(Config{
			Address:        addr,
			Peers:          peers,
			DataDir:        filepath.Join(t.TempDir(), "raft"),
			PromotionQuiet: promotionQuiet,
		}, nil)
		require.NoError(t, err)
		nodes[i] = node
		t.Cleanup(func() { assert.NoError(t, node.Close()) })
	}
	return nodes
}

// leaderOf races WaitToBecomeLeader across the nodes and reports which one
// won within the deadline. The others keep waiting until they are closed.
func leaderOf(t *testing.T, nodes []*Raft) int {
	t.Helper()

	won := make(chan int, len(nodes))
	for i, node := range nodes {
		p := NewProvider(t.Context(), node, metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled)
		go func() {
			if _, err := p.WaitToBecomeLeader(); err == nil {
				won <- i
			}
		}()
	}

	select {
	case i := <-won:
		return i
	case <-time.After(30 * time.Second):
		t.Fatal("no node became leader")
		return -1
	}
}

func voters(members []Member) []string {
	var ids []string
	for _, m := range members {
		if m.Voter {
			ids = append(ids, m.Address)
		}
	}
	return ids
}

// Three fresh nodes share one peer list. Only the first founds the cluster,
// so exactly one leader emerges; the leader then adds the others and
// promotes them once they prove reachable, until every peer votes.
func TestPeersFoundOnceAndGrowToVoters(t *testing.T) {
	peers := freeAddresses(t, 3)
	nodes := startPeers(t, peers, 200*time.Millisecond)

	leader := leaderOf(t, nodes)
	assert.Equal(t, 0, leader, "the first peer founds the cluster and leads it")

	require.Eventually(t, func() bool {
		members, err := nodes[leader].Members()
		return err == nil && len(voters(members)) == len(peers)
	}, 30*time.Second, 100*time.Millisecond, "every peer becomes a voter")

	members, err := nodes[leader].Members()
	require.NoError(t, err)
	assert.ElementsMatch(t, peers, voters(members))
}

// A peer that is not the founder never bootstraps on its own: started alone
// it has no configuration and waits; once the founder starts, it is added.
func TestNonFounderWaitsForTheFounder(t *testing.T) {
	peers := freeAddresses(t, 2)
	dirs := []string{filepath.Join(t.TempDir(), "raft"), filepath.Join(t.TempDir(), "raft")}

	second, err := New(Config{Address: peers[1], Peers: peers, DataDir: dirs[1], PromotionQuiet: 200 * time.Millisecond}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, second.Close()) })

	members, err := second.Members()
	require.NoError(t, err)
	assert.Empty(t, members, "no configuration until a leader adds this node")

	first, err := New(Config{Address: peers[0], Peers: peers, DataDir: dirs[0], PromotionQuiet: 200 * time.Millisecond}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, first.Close()) })

	require.Eventually(t, func() bool {
		members, err := first.Members()
		return err == nil && len(voters(members)) == 2
	}, 30*time.Second, 100*time.Millisecond, "the founder adds and promotes the peer that started first")
}

// Restarting the group on the same directories with a shorter list does not
// bootstrap again — the log already holds the configuration — and the
// leader removes the peer that is no longer listed.
func TestUnlistedPeerIsRemovedOnRestart(t *testing.T) {
	peers := freeAddresses(t, 3)
	dirs := make([]string, len(peers))
	for i := range dirs {
		dirs[i] = filepath.Join(t.TempDir(), "raft")
	}

	nodes := make([]*Raft, len(peers))
	for i, addr := range peers {
		node, err := New(Config{Address: addr, Peers: peers, DataDir: dirs[i], PromotionQuiet: 200 * time.Millisecond}, nil)
		require.NoError(t, err)
		nodes[i] = node
	}
	leader := leaderOf(t, nodes)
	require.Eventually(t, func() bool {
		members, err := nodes[leader].Members()
		return err == nil && len(voters(members)) == 3
	}, 30*time.Second, 100*time.Millisecond)
	for _, node := range nodes {
		require.NoError(t, node.Close())
	}

	shorter := peers[:2]
	restarted := make([]*Raft, len(shorter))
	for i, addr := range shorter {
		node, err := New(Config{Address: addr, Peers: shorter, DataDir: dirs[i], PromotionQuiet: 200 * time.Millisecond}, nil)
		require.NoError(t, err)
		restarted[i] = node
		t.Cleanup(func() { assert.NoError(t, node.Close()) })
	}
	leader = leaderOf(t, restarted)
	require.Eventually(t, func() bool {
		members, err := restarted[leader].Members()
		if err != nil {
			return false
		}
		addrs := make([]string, 0, len(members))
		for _, m := range members {
			addrs = append(addrs, m.Address)
		}
		return assert.ObjectsAreEqual(shorter, addrs) || assert.ObjectsAreEqual([]string{shorter[1], shorter[0]}, addrs)
	}, 30*time.Second, 100*time.Millisecond, "the dropped peer leaves the configuration")
}

// Every node names the leader by its raft address once one is elected —
// what a follower's redirect hint carries.
func TestFollowersNameTheLeader(t *testing.T) {
	peers := freeAddresses(t, 2)
	nodes := startPeers(t, peers, 200*time.Millisecond)
	leader := leaderOf(t, nodes)

	require.Eventually(t, func() bool {
		return nodes[1-leader].LeaderID() == peers[leader]
	}, 30*time.Second, 100*time.Millisecond)

	follower := NewProvider(t.Context(), nodes[1-leader], metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled)
	name, err := follower.GetLeaderName()
	require.NoError(t, err)
	assert.Equal(t, peers[leader], name)
}

// A listed peer that never answers is added as a non-voter and stays one:
// promoting it would add a vote nobody can cast.
func TestUnreachablePeerIsNeverPromoted(t *testing.T) {
	peers := freeAddresses(t, 2) // peers[1] is never started
	founder, err := New(Config{Address: peers[0], Peers: peers, DataDir: filepath.Join(t.TempDir(), "raft"), PromotionQuiet: 200 * time.Millisecond}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, founder.Close()) })
	leaderOf(t, []*Raft{founder})

	require.Eventually(t, func() bool {
		members, err := founder.Members()
		return err == nil && len(members) == 2
	}, 30*time.Second, 100*time.Millisecond, "the absent peer is added as a non-voter")

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		members, err := founder.Members()
		require.NoError(t, err)
		assert.Equal(t, []string{peers[0]}, voters(members), "only the founder votes")
		time.Sleep(200 * time.Millisecond)
	}
}
