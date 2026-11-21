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
	"log/slog"
	"sync"
	"time"

	hashicorpraft "github.com/hashicorp/raft"
	"github.com/pkg/errors"

	"github.com/oxia-db/oxia/common/failpoint"
)

// Config describes one node of the coordinator's raft group.
type Config struct {
	// Address is where this node binds and advertises the raft traffic; it is
	// also the identity of the node in the group.
	Address string

	// Peers is the whole group, as raft addresses, in one order shared by
	// every node. The membership follows the list: the first peer founds the
	// group when it has no state, and whichever peer leads adds the listed
	// nodes as non-voters, promotes them once they prove reachable, and
	// removes the nodes that are no longer listed. A node absent from its own
	// list is retiring: it founds and joins nothing, and hands the leadership
	// to a listed voter whenever it holds it. Mutually exclusive with
	// BootstrapNodes.
	Peers []string

	// BootstrapNodes is the fixed membership: every node writes the same
	// initial configuration when it has no state, and the membership never
	// changes afterwards.
	BootstrapNodes []string

	// DataDir holds the log, the stable store and the snapshots of the node.
	DataDir string

	// PromotionQuiet is the least time a listed non-voter stays one before the
	// leader promotes it to voter; zero is twice the heartbeat timeout. The
	// promotion also needs a heartbeat timeout free of failed heartbeats to
	// the node: promoting an unreachable node would add a vote that nobody
	// can cast.
	PromotionQuiet time.Duration
}

// Member is one server of the configuration of the group.
type Member struct {
	Address string
	Voter   bool
}

// Membership is the raft group as one node sees it: what an operator reads to
// know when the new peers of the group vote, and which one leads.
type Membership interface {
	Members() ([]Member, error)
	LeaderID() string
}

const (
	reconcileInterval       = time.Second
	membershipChangeTimeout = 10 * time.Second
)

func (c Config) validate() error {
	if len(c.Peers) > 0 && len(c.BootstrapNodes) > 0 {
		return errors.New("raft peers and bootstrap nodes are mutually exclusive")
	}
	return nil
}

// founds reports whether this node writes the first configuration of the group
// when it has no state: every node does with the fixed membership, and only
// the first peer does with a peer list.
func (c Config) founds() bool {
	return len(c.Peers) == 0 || c.Peers[0] == c.Address
}

// initialConfiguration is what a founder bootstraps the group with.
func (c Config) initialConfiguration() hashicorpraft.Configuration {
	if len(c.Peers) == 0 {
		return hashicorpraft.Configuration{Servers: getRaftServers(c.BootstrapNodes)}
	}
	return hashicorpraft.Configuration{Servers: getRaftServers([]string{c.Address})}
}

func (c Config) promotionQuiet(heartbeatTimeout time.Duration) time.Duration {
	if c.PromotionQuiet > 0 {
		return c.PromotionQuiet
	}
	return 2 * heartbeatTimeout
}

// membership keeps the raft configuration equal to the peer list while this
// node leads. It owns the heartbeat bookkeeping that the promotion rule reads.
type membership struct {
	node             *hashicorpraft.Raft
	self             hashicorpraft.ServerID
	peers            []string
	promotionQuiet   time.Duration
	heartbeatTimeout time.Duration
	logger           *slog.Logger
	failpoints       *failpoint.Failpoints

	observations chan hashicorpraft.Observation
	observer     *hashicorpraft.Observer

	// seen is when a listed non-voter first came under the care of this
	// leader, and failed is its last failed heartbeat. Both belong to the
	// reconciler goroutine alone.
	seen   map[hashicorpraft.ServerID]time.Time
	failed map[hashicorpraft.ServerID]time.Time

	stop chan struct{}
	done sync.WaitGroup
}

func startMembership(node *hashicorpraft.Raft, cfg Config, heartbeatTimeout time.Duration, logger *slog.Logger,
	failpoints *failpoint.Failpoints) *membership {
	m := &membership{
		node:             node,
		failpoints:       failpoints,
		self:             hashicorpraft.ServerID(cfg.Address),
		peers:            cfg.Peers,
		promotionQuiet:   cfg.promotionQuiet(heartbeatTimeout),
		heartbeatTimeout: heartbeatTimeout,
		logger:           logger,
		observations:     make(chan hashicorpraft.Observation, 64),
		seen:             make(map[hashicorpraft.ServerID]time.Time),
		failed:           make(map[hashicorpraft.ServerID]time.Time),
		stop:             make(chan struct{}),
	}
	m.observer = hashicorpraft.NewObserver(m.observations, false, isHeartbeatObservation)
	node.RegisterObserver(m.observer)

	m.done.Go(m.run)
	return m
}

// isHeartbeatObservation keeps the observations the promotion rule reads.
func isHeartbeatObservation(o *hashicorpraft.Observation) bool {
	switch o.Data.(type) {
	case hashicorpraft.FailedHeartbeatObservation, hashicorpraft.ResumedHeartbeatObservation:
		return true
	default:
		return false
	}
}

func (m *membership) close() {
	close(m.stop)
	m.done.Wait()
	m.node.DeregisterObserver(m.observer)
}

func (m *membership) run() {
	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case o := <-m.observations:
			if failed, ok := o.Data.(hashicorpraft.FailedHeartbeatObservation); ok {
				m.failed[failed.PeerID] = time.Now()
			}
		case <-ticker.C:
			if m.node.State() == hashicorpraft.Leader {
				m.reconcile(time.Now())
			}
		case <-m.stop:
			return
		}
	}
}

// reconcile applies one round of the membership rules to the current
// configuration. Every change is one committed configuration entry, so a
// failure is retried on the next tick rather than reported.
func (m *membership) reconcile(now time.Time) {
	servers, err := configuration(m.node, m.failpoints)
	if err != nil {
		m.logger.Warn("Failed to read the raft configuration", slog.Any("error", err))
		return
	}

	listed := make(map[hashicorpraft.ServerID]bool, len(m.peers))
	for _, p := range m.peers {
		listed[hashicorpraft.ServerID(p)] = true
	}
	present := make(map[hashicorpraft.ServerID]bool, len(servers))
	for _, s := range servers {
		present[s.ID] = true
	}

	if !listed[m.self] && m.handOff(servers, listed) {
		return
	}

	for _, p := range m.peers {
		id := hashicorpraft.ServerID(p)
		if present[id] {
			continue
		}
		m.logger.Info("Adding a peer as non-voter", slog.String("peer", p))
		if err := m.node.AddNonvoter(id, hashicorpraft.ServerAddress(p), 0, membershipChangeTimeout).Error(); err != nil {
			m.logger.Warn("Failed to add a peer as non-voter", slog.String("peer", p), slog.Any("error", err))
			return
		}
		m.seen[id] = now
	}

	for _, s := range servers {
		switch {
		case s.ID == m.self:
			continue
		case !listed[s.ID]:
			m.logger.Info("Removing a server that is no longer a peer", slog.String("server", string(s.ID)))
			if err := m.node.RemoveServer(s.ID, 0, membershipChangeTimeout).Error(); err != nil {
				m.logger.Warn("Failed to remove a server", slog.String("server", string(s.ID)), slog.Any("error", err))
				return
			}
			delete(m.seen, s.ID)
			delete(m.failed, s.ID)
		case s.Suffrage == hashicorpraft.Nonvoter && m.reachableSince(s.ID, now):
			m.logger.Info("Promoting a peer to voter", slog.String("peer", string(s.ID)))
			if err := m.node.AddVoter(s.ID, s.Address, 0, membershipChangeTimeout).Error(); err != nil {
				m.logger.Warn("Failed to promote a peer", slog.String("peer", string(s.ID)), slog.Any("error", err))
				return
			}
		default:
			// A listed voter, or a listed non-voter still proving itself
		}
	}
}

// handOff is the rule of a retiring leader: a leader that its own list
// disowns hands the leadership to a listed voter, whose list then removes it,
// and reports whether it tried. Until a listed voter exists it keeps leading:
// the listed peers still need adding and promoting, and reconcile never
// removes the leader itself.
func (m *membership) handOff(servers []hashicorpraft.Server, listed map[hashicorpraft.ServerID]bool) bool {
	for _, s := range servers {
		if s.ID == m.self || !listed[s.ID] || s.Suffrage != hashicorpraft.Voter {
			continue
		}
		m.logger.Info("Handing the leadership to a listed voter: this node is not in its own peer list",
			slog.String("to", string(s.ID)))
		if err := m.node.LeadershipTransferToServer(s.ID, s.Address).Error(); err != nil {
			m.logger.Warn("Failed to hand the leadership over", slog.String("to", string(s.ID)), slog.Any("error", err))
		}
		return true
	}
	return false
}

// reachableSince reports whether id is old enough to promote and answers the
// heartbeats: at least the quiet period under the care of this leader, and no
// failed heartbeat within one heartbeat timeout. An unreachable node fails a
// heartbeat at least that often, whatever the backoff.
func (m *membership) reachableSince(id hashicorpraft.ServerID, now time.Time) bool {
	since, ok := m.seen[id]
	if !ok {
		// Inherited from a previous leader: the clock starts now
		m.seen[id] = now
		return false
	}
	if now.Sub(since) < m.promotionQuiet {
		return false
	}
	failed, ok := m.failed[id]
	return !ok || now.Sub(failed) >= m.heartbeatTimeout
}

// Members reports the current configuration of the group.
func (r *Raft) Members() ([]Member, error) {
	servers, err := configuration(r.node, r.sc.failpoints)
	if err != nil {
		return nil, err
	}
	members := make([]Member, 0, len(servers))
	for _, s := range servers {
		members = append(members, Member{Address: string(s.Address), Voter: s.Suffrage == hashicorpraft.Voter})
	}
	return members, nil
}

// configuration reads the servers of the latest configuration of the group.
func configuration(node *hashicorpraft.Raft, failpoints *failpoint.Failpoints) ([]hashicorpraft.Server, error) {
	future := node.GetConfiguration()
	if err := failpoints.Fail("membership.configuration", future.Error()); err != nil {
		return nil, errors.Wrap(err, "failed to read the raft configuration")
	}
	return future.Configuration().Servers, nil
}

// LeaderID is the identity of the current leader, empty when it is unknown.
func (r *Raft) LeaderID() string {
	_, id := r.node.LeaderWithID()
	return string(id)
}

var _ Membership = (*Raft)(nil)
