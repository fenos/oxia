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
	"slices"
	"sync"
	"time"

	hashicorpraft "github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

// Config describes one node of the coordinator's raft group.
type Config struct {
	// Address is where this node binds and advertises raft traffic; it is
	// also the node's identity in the group.
	Address string

	// Peers is the whole group, as raft addresses, in one order shared by
	// every node. Membership follows the list: the first peer founds the
	// group when it has no state, and whichever peer leads adds listed
	// nodes as non-voters, promotes them once they prove reachable, and
	// removes nodes that are no longer listed. Address must be listed.
	// Mutually exclusive with BootstrapNodes.
	Peers []string

	// BootstrapNodes is the fixed-membership form: every node writes the
	// same initial configuration when it has no state, and membership
	// never changes afterwards.
	BootstrapNodes []string

	// DataDir holds the node's log, stable store and snapshots.
	DataDir string

	// PromotionQuiet is the least time a listed non-voter stays one before
	// the leader promotes it to voter; zero uses twice the heartbeat
	// timeout. Promotion also needs a heartbeat timeout free of failed
	// heartbeats to the node: promoting an unreachable node would add a
	// vote nobody can cast.
	PromotionQuiet time.Duration
}

// Member is one server of the group's configuration.
type Member struct {
	Address string
	Voter   bool
}

const (
	reconcileInterval       = time.Second
	membershipChangeTimeout = 10 * time.Second
)

func (c Config) validate() error {
	if c.Address == "" {
		return errors.New("raft address must be set")
	}
	if len(c.Peers) > 0 && len(c.BootstrapNodes) > 0 {
		return errors.New("raft peers and bootstrap nodes are mutually exclusive")
	}
	if len(c.Peers) > 0 && !slices.Contains(c.Peers, c.Address) {
		return errors.Errorf("raft address %q is not among the peers", c.Address)
	}
	return nil
}

// founds reports whether this node writes the group's first configuration
// when it has no state: every node does under BootstrapNodes, only the
// first peer does under Peers.
func (c Config) founds() bool {
	return len(c.BootstrapNodes) > 0 || (len(c.Peers) > 0 && c.Peers[0] == c.Address)
}

// initialConfiguration is what a founder bootstraps with.
func (c Config) initialConfiguration() hashicorpraft.Configuration {
	if len(c.BootstrapNodes) > 0 {
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
// node leads. It owns the heartbeat bookkeeping the promotion rule reads.
type membership struct {
	node             *hashicorpraft.Raft
	self             hashicorpraft.ServerID
	peers            []string
	promotionQuiet   time.Duration
	heartbeatTimeout time.Duration
	logger           *slog.Logger

	observations chan hashicorpraft.Observation
	observer     *hashicorpraft.Observer

	// seen is when a listed non-voter first came under this leader's care;
	// failed is its last failed heartbeat. Both are the reconciler's alone.
	seen   map[hashicorpraft.ServerID]time.Time
	failed map[hashicorpraft.ServerID]time.Time

	stop chan struct{}
	done sync.WaitGroup
}

func startMembership(node *hashicorpraft.Raft, cfg Config, heartbeatTimeout time.Duration, logger *slog.Logger) *membership {
	m := &membership{
		node:             node,
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
	m.observer = hashicorpraft.NewObserver(m.observations, false, func(o *hashicorpraft.Observation) bool {
		switch o.Data.(type) {
		case hashicorpraft.FailedHeartbeatObservation, hashicorpraft.ResumedHeartbeatObservation:
			return true
		default:
			return false
		}
	})
	node.RegisterObserver(m.observer)

	m.done.Add(1)
	go m.run()
	return m
}

func (m *membership) close() {
	close(m.stop)
	m.done.Wait()
	m.node.DeregisterObserver(m.observer)
}

func (m *membership) run() {
	defer m.done.Done()
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

// reconcile applies one round of the membership rules against the current
// configuration. Every change is one committed configuration entry, so a
// failure here is retried on the next tick rather than reported.
func (m *membership) reconcile(now time.Time) {
	future := m.node.GetConfiguration()
	if err := future.Error(); err != nil {
		m.logger.Warn("Failed to read the raft configuration", slog.Any("error", err))
		return
	}
	servers := future.Configuration().Servers

	listed := make(map[hashicorpraft.ServerID]bool, len(m.peers))
	for _, p := range m.peers {
		listed[hashicorpraft.ServerID(p)] = true
	}
	present := make(map[hashicorpraft.ServerID]hashicorpraft.Server, len(servers))
	for _, s := range servers {
		present[s.ID] = s
	}

	for _, p := range m.peers {
		id := hashicorpraft.ServerID(p)
		if _, ok := present[id]; ok {
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
			// A listed voter, or a listed non-voter still proving itself.
		}
	}
}

// reachableSince reports whether id is old enough to promote and answers
// heartbeats: at least the quiet period under this leader's care, and no
// failed heartbeat within one heartbeat timeout — an unreachable node
// fails a heartbeat at least that often, whatever the backoff.
func (m *membership) reachableSince(id hashicorpraft.ServerID, now time.Time) bool {
	since, ok := m.seen[id]
	if !ok {
		// Inherited from a previous leader: start the clock now.
		m.seen[id] = now
		return false
	}
	if now.Sub(since) < m.promotionQuiet {
		return false
	}
	failed, ok := m.failed[id]
	return !ok || now.Sub(failed) >= m.heartbeatTimeout
}

// Members reports the group's current configuration.
func (r *Raft) Members() ([]Member, error) {
	future := r.node.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, errors.Wrap(err, "failed to read the raft configuration")
	}
	servers := future.Configuration().Servers
	members := make([]Member, 0, len(servers))
	for _, s := range servers {
		members = append(members, Member{Address: string(s.Address), Voter: s.Suffrage == hashicorpraft.Voter})
	}
	return members, nil
}

// LeaderID is the identity of the current leader, empty when unknown.
func (r *Raft) LeaderID() string {
	_, id := r.node.LeaderWithID()
	return string(id)
}
