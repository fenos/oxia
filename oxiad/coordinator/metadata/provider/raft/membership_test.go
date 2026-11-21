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
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"testing"
	"time"

	hashicorpraft "github.com/hashicorp/raft"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/failpoint"
	metadatacommon "github.com/oxia-db/oxia/oxiad/common/metadata"
	metadatacodec "github.com/oxia-db/oxia/oxiad/common/metadata/codec"
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

// startPeers brings up one raft node per address, every one with the same peer
// list, and returns them in the order of the list.
func startPeers(t *testing.T, peers []string, promotionQuiet time.Duration) []*Raft {
	t.Helper()

	nodes := make([]*Raft, len(peers))
	for i, addr := range peers {
		node, err := NewFromConfig(Config{
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

// leaderOf races WaitToBecomeLeader across the nodes and reports which one won
// within the deadline. The others keep waiting until they are closed.
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

// awaitVoters waits until the configuration seen by node has n voters.
func awaitVoters(t *testing.T, node *Raft, n int) {
	t.Helper()

	require.Eventually(t, func() bool {
		members, err := node.Members()
		return err == nil && len(voters(members)) == n
	}, 30*time.Second, 100*time.Millisecond, "the group has %d voters", n)
}

func TestNewRejectsPeersWithBootstrapNodes(t *testing.T) {
	addr := freeAddresses(t, 1)[0]
	_, err := NewFromConfig(Config{
		Address:        addr,
		Peers:          []string{addr},
		BootstrapNodes: []string{addr},
		DataDir:        t.TempDir(),
	}, nil)
	require.ErrorContains(t, err, "mutually exclusive")
}

// Three fresh nodes share one peer list. Only the first founds the group, so
// exactly one leader emerges; the leader then adds the others and promotes
// them once they prove reachable, until every peer votes.
func TestPeersFoundOnceAndGrowToVoters(t *testing.T) {
	peers := freeAddresses(t, 3)
	nodes := startPeers(t, peers, 200*time.Millisecond)

	leader := leaderOf(t, nodes)
	assert.Equal(t, 0, leader, "the first peer founds the group and leads it")
	awaitVoters(t, nodes[leader], len(peers))

	members, err := nodes[leader].Members()
	require.NoError(t, err)
	assert.ElementsMatch(t, peers, voters(members))
}

// A peer that is not the founder never bootstraps on its own: started alone it
// has no configuration and waits; once the founder starts, it is added.
func TestNonFounderWaitsForTheFounder(t *testing.T) {
	peers := freeAddresses(t, 2)
	dirs := []string{filepath.Join(t.TempDir(), "raft"), filepath.Join(t.TempDir(), "raft")}

	second, err := NewFromConfig(Config{Address: peers[1], Peers: peers, DataDir: dirs[1], PromotionQuiet: 200 * time.Millisecond}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, second.Close()) })

	members, err := second.Members()
	require.NoError(t, err)
	assert.Empty(t, members, "no configuration until a leader adds this node")

	first, err := NewFromConfig(Config{Address: peers[0], Peers: peers, DataDir: dirs[0], PromotionQuiet: 200 * time.Millisecond}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, first.Close()) })

	awaitVoters(t, first, 2)
}

// Restarting the group on the same directories with a shorter list does not
// bootstrap again, as the log already holds the configuration, and the leader
// removes the peer that is no longer listed.
func TestUnlistedPeerIsRemovedOnRestart(t *testing.T) {
	peers := freeAddresses(t, 3)
	dirs := make([]string, len(peers))
	for i := range dirs {
		dirs[i] = filepath.Join(t.TempDir(), "raft")
	}

	nodes := make([]*Raft, len(peers))
	for i, addr := range peers {
		node, err := NewFromConfig(Config{Address: addr, Peers: peers, DataDir: dirs[i], PromotionQuiet: 200 * time.Millisecond}, nil)
		require.NoError(t, err)
		nodes[i] = node
	}
	leader := leaderOf(t, nodes)
	awaitVoters(t, nodes[leader], 3)
	for _, node := range nodes {
		require.NoError(t, node.Close())
	}

	shorter := peers[:2]
	restarted := make([]*Raft, len(shorter))
	for i, addr := range shorter {
		node, err := NewFromConfig(Config{Address: addr, Peers: shorter, DataDir: dirs[i], PromotionQuiet: 200 * time.Millisecond}, nil)
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

// Every node names the leader by its raft address once one is elected: what
// the redirect of a follower carries.
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
// promoting it would add a vote that nobody can cast.
func TestUnreachablePeerIsNeverPromoted(t *testing.T) {
	peers := freeAddresses(t, 2) // peers[1] is never started
	founder, err := NewFromConfig(Config{Address: peers[0], Peers: peers, DataDir: filepath.Join(t.TempDir(), "raft"), PromotionQuiet: 200 * time.Millisecond}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, founder.Close()) })
	leaderOf(t, []*Raft{founder})

	require.Eventually(t, func() bool {
		members, err := founder.Members()
		return err == nil && len(members) == 2
	}, 30*time.Second, 100*time.Millisecond, "the absent peer is added as a non-voter")

	assert.Never(t, func() bool {
		members, err := founder.Members()
		return err != nil || len(voters(members)) != 1
	}, 3*time.Second, 200*time.Millisecond, "only the founder votes")
}

// A leader whose own list no longer names it is retiring. It hands the
// leadership to a listed voter instead of keeping it; whether the list of the
// new leader then removes it is the rule of that leader, so here the others
// keep listing the retiring node, and the handoff alone is what shows.
func TestLeaderAbsentFromItsListHandsOff(t *testing.T) {
	peers := freeAddresses(t, 3)
	dirs := make([]string, len(peers))
	for i := range dirs {
		dirs[i] = filepath.Join(t.TempDir(), "raft")
	}

	nodes := make([]*Raft, len(peers))
	for i, addr := range peers {
		node, err := NewFromConfig(Config{Address: addr, Peers: peers, DataDir: dirs[i], PromotionQuiet: 200 * time.Millisecond}, nil)
		require.NoError(t, err)
		nodes[i] = node
	}
	leader := leaderOf(t, nodes)
	awaitVoters(t, nodes[leader], 3)
	for _, node := range nodes {
		require.NoError(t, node.Close())
	}

	// The first node restarts absent from its own list; the others still
	// list everyone
	lists := [][]string{peers[1:], peers, peers}
	restarted := make([]*Raft, len(peers))
	for i, addr := range peers {
		node, err := NewFromConfig(Config{Address: addr, Peers: lists[i], DataDir: dirs[i], PromotionQuiet: 200 * time.Millisecond}, nil)
		require.NoError(t, err)
		restarted[i] = node
		t.Cleanup(func() { assert.NoError(t, node.Close()) })
	}
	leader = leaderOf(t, restarted)

	// The retiring node leads, so that the handoff is what the assertion
	// exercises
	if leader != 0 {
		require.NoError(t, restarted[leader].node.LeadershipTransferToServer(
			hashicorpraft.ServerID(peers[0]), hashicorpraft.ServerAddress(peers[0])).Error())
	}
	require.Eventually(t, func() bool { return restarted[0].node.State() == hashicorpraft.Leader }, 30*time.Second, 100*time.Millisecond)

	require.Eventually(t, func() bool {
		return restarted[0].node.State() != hashicorpraft.Leader &&
			(restarted[1].node.State() == hashicorpraft.Leader || restarted[2].node.State() == hashicorpraft.Leader)
	}, 30*time.Second, 100*time.Millisecond, "the retiring leader handed the leadership to a listed voter")

	// It handed over, it did not leave: the others still list it
	for _, i := range []int{1, 2} {
		if restarted[i].node.State() == hashicorpraft.Leader {
			members, err := restarted[i].Members()
			require.NoError(t, err)
			require.Len(t, voters(members), 3)
		}
	}
}

// Every change that a node makes while it doesn't lead fails, and is left to
// the next round: here the rounds run on a follower, so that each change is
// refused. The group lists a third peer that never starts: the leader adds it
// as a non-voter, and never promotes it.
func TestReconcileLeavesAFailedChangeToTheNextRound(t *testing.T) {
	peers := freeAddresses(t, 3)
	nodes := make([]*Raft, 2)
	for i, addr := range peers[:2] {
		node, err := NewFromConfig(Config{Address: addr, Peers: peers, DataDir: filepath.Join(t.TempDir(), "raft"),
			PromotionQuiet: 200 * time.Millisecond}, nil)
		require.NoError(t, err)
		nodes[i] = node
		t.Cleanup(func() { assert.NoError(t, node.Close()) })
	}
	leader := leaderOf(t, nodes)
	follower := nodes[1-leader]
	require.Eventually(t, func() bool {
		members, err := follower.Members()
		return err == nil && len(members) == 3 && len(voters(members)) == 2
	}, 30*time.Second, 100*time.Millisecond, "the follower knows two voters and the non-voter")
	self, other, nonVoter := peers[1-leader], peers[leader], peers[2]
	absent := freeAddresses(t, 1)[0]

	round := func(peers []string, seen map[hashicorpraft.ServerID]time.Time) *membership {
		m := &membership{
			node:             follower.node,
			self:             hashicorpraft.ServerID(self),
			peers:            peers,
			promotionQuiet:   time.Millisecond,
			heartbeatTimeout: time.Millisecond,
			logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
			seen:             seen,
			failed:           map[hashicorpraft.ServerID]time.Time{},
		}
		m.reconcile(time.Now())
		return m
	}

	// A listed peer missing from the configuration can't be added
	m := round([]string{self, other, nonVoter, absent}, map[hashicorpraft.ServerID]time.Time{})
	assert.NotContains(t, m.seen, hashicorpraft.ServerID(absent))

	// A configured server that is no longer listed can't be removed
	m = round([]string{self}, map[hashicorpraft.ServerID]time.Time{hashicorpraft.ServerID(other): time.Now()})
	assert.Contains(t, m.seen, hashicorpraft.ServerID(other))

	// A non-voter past its quiet period can't be promoted
	round(peers, map[hashicorpraft.ServerID]time.Time{hashicorpraft.ServerID(nonVoter): time.Now().Add(-time.Hour)})

	// A retiring node can't hand over a leadership it doesn't hold
	round([]string{other}, map[hashicorpraft.ServerID]time.Time{})

	members, err := follower.Members()
	require.NoError(t, err)
	assert.Len(t, members, 3, "nothing changed")
	assert.Len(t, voters(members), 2, "nothing changed")
}

// The quiet period of a non-voter that came under the care of a previous
// leader starts with the first round of this one, and a failed heartbeat holds
// the promotion for one heartbeat timeout.
func TestReachableSince(t *testing.T) {
	id := hashicorpraft.ServerID("peer")
	m := &membership{
		promotionQuiet:   time.Minute,
		heartbeatTimeout: time.Second,
		seen:             map[hashicorpraft.ServerID]time.Time{},
		failed:           map[hashicorpraft.ServerID]time.Time{},
	}
	now := time.Now()
	assert.False(t, m.reachableSince(id, now), "the clock starts with the first round")
	assert.False(t, m.reachableSince(id, now.Add(time.Second)), "still quiet")
	assert.True(t, m.reachableSince(id, now.Add(time.Minute)), "quiet long enough, no failed heartbeat")

	m.failed[id] = now.Add(time.Minute)
	assert.False(t, m.reachableSince(id, now.Add(time.Minute)), "a failed heartbeat holds the promotion")
	assert.True(t, m.reachableSince(id, now.Add(time.Minute+time.Second)), "one heartbeat timeout later")
}

// A leader that its own list disowns, with no listed voter to hand the
// leadership to yet, keeps leading and adds the listed peers.
func TestRetiringLeaderWithoutAListedVoterAddsThePeers(t *testing.T) {
	addrs := freeAddresses(t, 2) // addrs[1] is never started
	founder, err := NewFromConfig(Config{Address: addrs[0], Peers: addrs[:1], DataDir: filepath.Join(t.TempDir(), "raft")}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, founder.Close()) })
	leaderOf(t, []*Raft{founder})

	m := &membership{
		node:   founder.node,
		self:   hashicorpraft.ServerID(addrs[0]),
		peers:  addrs[1:],
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		seen:   map[hashicorpraft.ServerID]time.Time{},
		failed: map[hashicorpraft.ServerID]time.Time{},
	}
	m.reconcile(time.Now())
	assert.Contains(t, m.seen, hashicorpraft.ServerID(addrs[1]), "the listed peer was added")
}

// A failure to read the configuration fails Members, and a round of the
// membership rules changes nothing.
func TestConfigurationReadFailure(t *testing.T) {
	if !failpoint.Enabled {
		t.Skip("needs failpoints: disabled by the disable_trap tag")
	}
	addrs := freeAddresses(t, 2)
	failed := errors.New("failpoint")
	node, err := newRaft(Config{Address: addrs[0], Peers: addrs, DataDir: filepath.Join(t.TempDir(), "raft")}, nil,
		failpoint.New(map[string]func() error{"membership.configuration": func() error { return failed }}))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, node.Close()) })

	_, err = node.Members()
	require.ErrorIs(t, err, failed)

	m := &membership{node: node.node, failpoints: node.sc.failpoints, peers: addrs,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)), seen: map[hashicorpraft.ServerID]time.Time{}}
	m.reconcile(time.Now())
	assert.Empty(t, m.seen)
}

func TestPromotionQuietDefaultsToTwoHeartbeatTimeouts(t *testing.T) {
	assert.Equal(t, 10*time.Second, Config{}.promotionQuiet(5*time.Second))
	assert.Equal(t, time.Second, Config{PromotionQuiet: time.Second}.promotionQuiet(5*time.Second))
}

func TestHeartbeatObservationsAreTheOnesKept(t *testing.T) {
	assert.True(t, isHeartbeatObservation(&hashicorpraft.Observation{Data: hashicorpraft.FailedHeartbeatObservation{}}))
	assert.True(t, isHeartbeatObservation(&hashicorpraft.Observation{Data: hashicorpraft.ResumedHeartbeatObservation{}}))
	assert.False(t, isHeartbeatObservation(&hashicorpraft.Observation{Data: hashicorpraft.LeaderObservation{}}))
}
