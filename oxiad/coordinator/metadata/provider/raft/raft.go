// Copyright 2023-2025 The Oxia Authors
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
	"context"
	"encoding/json"
	stderrors "errors"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/magodo/slog2hclog"
	"github.com/pkg/errors"
	"go.uber.org/multierr"

	commonproto "github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
)

type Provider struct {
	sync.Mutex

	sc    *stateContainer
	raft  *raft.Raft
	store *kvRaftStore
	log   *slog.Logger

	// fsmReadyOnce gates the FSM-catchup wait inside Get. hashicorp/raft
	// restores state asynchronously after NewRaft returns: a Get fired
	// before the restore finishes sees sc.State==nil, which lets
	// coordinator/metadata.persistStatusLocked believe the cluster is
	// fresh and mint a new InstanceId — overwriting the previously
	// persisted one and breaking dataserver↔coord handshakes that
	// authenticate by InstanceId. We block the first Get on
	// AppliedIndex catching up to LastIndex; subsequent Gets are
	// immediate (sync.Once short-circuits after the initial wait).
	fsmReadyOnce sync.Once
}

func (mpr *Provider) WaitToBecomeLeader(ctx context.Context) error {
	select {
	case <-mpr.raft.LeaderCh():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func NewProvider(
	raftAddress string,
	raftBootstrapNodes []string,
	raftDataDir string,
) (provider.Provider, error) {
	mpr := &Provider{
		sc:  newStateContainer(slog.With(slog.String("component", "metadata-provider-raft-state-container"))),
		log: slog.With(slog.String("component", "metadata-provider-raft")),
	}

	// Ensure data dir per node
	nodeId := raftAddress
	dataDir := filepath.Join(raftDataDir, nodeId)
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, errors.Wrap(err, "failed to create data dir")
	}

	// Setup Raft configuration. The four timeouts below MUST stay in
	// the relative ratio hashicorp/raft expects:
	//
	//   CommitTimeout < LeaderLeaseTimeout <= HeartbeatTimeout < ElectionTimeout
	//
	// LeaderLeaseTimeout is the only one that controls the
	// "failed to contact" leader-lease WARN: the leader logs it
	// whenever a follower's last-contact gap exceeds the lease.
	// hashicorp's default (500 ms) was paired with the default 1 s
	// HeartbeatTimeout — bumping HeartbeatTimeout to 5 s without
	// bumping the lease leaves the lease only ~1 heartbeat away from
	// any normal jitter, so cross-AZ / 6PN-mesh networks (~ tens of
	// ms but with occasional bursts) keep tripping the warning even
	// though the cluster is perfectly healthy. Raise the lease in
	// proportion (heartbeat / 2 = 2.5 s) so the warning fires only
	// on a genuine multi-heartbeat stall, and bump CommitTimeout
	// (heartbeat batch interval) in the same proportion.
	config := raft.DefaultConfig()
	config.HeartbeatTimeout = 5 * time.Second
	config.ElectionTimeout = 10 * time.Second
	config.LeaderLeaseTimeout = 2500 * time.Millisecond
	config.CommitTimeout = 250 * time.Millisecond
	// Snapshot more aggressively than the upstream defaults
	// (SnapshotInterval=120s, SnapshotThreshold=8192). The coord-raft
	// log carries small JSON cluster-status diffs, but on every cluster
	// restart the new leader replays the full log forward — followers
	// re-apply hundreds of entries and every replay logs a line. With
	// a 30s/1024-entry snapshot cadence the replay window stays bounded
	// and restarts are quiet + fast.
	config.SnapshotInterval = 30 * time.Second
	config.SnapshotThreshold = 1024
	config.LocalID = raft.ServerID(nodeId)
	config.LogLevel = "INFO"
	levelVar := &slog.LevelVar{}
	levelVar.Set(slog.LevelInfo)
	config.Logger = slog2hclog.New(mpr.log, levelVar)

	// Create TCP transport for Raft
	addr, err := net.ResolveTCPAddr("tcp", raftAddress)
	if err != nil {
		return nil, errors.Wrap(err, "failed to resolve raft address")
	}
	transport, err := raft.NewTCPTransport(raftAddress, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create raft transport")
	}

	// Create stable store and log store
	mpr.store, err = newKVRaftStore(filepath.Join(dataDir, "store"))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create data store")
	}

	// Create snapshot store
	snapshotStore, err := raft.NewFileSnapshotStoreWithLogger(dataDir, 2, config.Logger)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create snapshot store")
	}

	// Create Raft node
	mpr.raft, err = raft.NewRaft(config, mpr.sc, mpr.store, mpr.store, snapshotStore, transport)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create raft node")
	}

	if hasState, err := raft.HasExistingState(mpr.store, mpr.store, snapshotStore); err != nil {
		return nil, errors.Wrap(err, "failed to check existing state")
	} else if !hasState {
		configuration := raft.Configuration{
			Servers: getRaftServers(raftBootstrapNodes),
		}
		future := mpr.raft.BootstrapCluster(configuration)
		if err := future.Error(); err != nil {
			return nil, errors.Wrap(err, "failed to create raft node")
		}
	}

	return mpr, nil
}

func getRaftServers(bootstrapNodes []string) []raft.Server {
	servers := make([]raft.Server, len(bootstrapNodes))
	for i, addr := range bootstrapNodes {
		servers[i] = raft.Server{
			ID:      raft.ServerID(addr),
			Address: raft.ServerAddress(addr),
		}
	}
	return servers
}

func (mpr *Provider) Close() error {
	// Proactive leader handoff: if this node is currently the raft
	// leader, transfer leadership to a peer before shutting raft down.
	// Survivors get a leader immediately rather than waiting for
	// heartbeat-timeout to trigger an election (~5s in our default
	// config). Best-effort — if transfer fails (no eligible peer, all
	// followers behind, etc.) we log and fall through to Shutdown.
	if mpr.raft.State() == raft.Leader {
		if err := mpr.raft.LeadershipTransfer().Error(); err != nil {
			mpr.log.Warn("leadership transfer on close failed; falling back to shutdown",
				slog.Any("error", err))
		} else {
			mpr.log.Info("leadership transferred on close")
		}
	}
	return multierr.Combine(
		mpr.raft.Shutdown().Error(),
		mpr.store.Close(),
	)
}

func toVersion(v int64) provider.Version {
	return provider.Version(strconv.FormatInt(v, 10))
}

func fromVersion(v provider.Version) int64 {
	n, _ := strconv.ParseInt(string(v), 10, 64)
	return n
}

func (mpr *Provider) Get() (cs *commonproto.ClusterStatus, version provider.Version, err error) {
	mpr.fsmReadyOnce.Do(mpr.waitFSMReady)

	mpr.Lock()
	defer mpr.Unlock()

	mpr.log.Debug("Get metadata",
		slog.Any("cluster-status", mpr.sc.State),
		slog.Any("current-version", mpr.sc.CurrentVersion))
	return mpr.sc.State, toVersion(mpr.sc.CurrentVersion), nil
}

// waitFSMReady blocks until raft has applied every persisted log
// entry to the FSM, capped at 30 s. Called once via fsmReadyOnce so
// only the first Get pays the wait — every subsequent Get is a plain
// in-memory read.
//
// The cap exists to handle pathological "follower never catches up"
// scenarios gracefully; in those cases the caller (typically
// coordinator/metadata.doStatusRecovery) proceeds with the current
// state, and Store's CAS-with-refresh handles whatever drift remains.
//
// Lock note: this method does NOT take mpr.Lock — sleeping while
// holding it would block Apply. Apply itself doesn't take Provider's
// mutex (it operates on stateContainer), so leaving the lock free
// here is correct.
func (mpr *Provider) waitFSMReady() {
	const (
		readyWaitBudget = 30 * time.Second
		readyPollEvery  = 20 * time.Millisecond
	)
	deadline := time.Now().Add(readyWaitBudget)
	for {
		applied := mpr.raft.AppliedIndex()
		last := mpr.raft.LastIndex()
		if applied >= last {
			mpr.log.Info("FSM caught up with raft log",
				slog.Uint64("applied-index", applied),
				slog.Uint64("last-index", last))
			return
		}
		if time.Now().After(deadline) {
			mpr.log.Warn("FSM still behind raft log after wait budget — proceeding anyway",
				slog.Uint64("applied-index", applied),
				slog.Uint64("last-index", last),
				slog.Duration("budget", readyWaitBudget))
			return
		}
		time.Sleep(readyPollEvery)
	}
}

func (mpr *Provider) Store(cs *commonproto.ClusterStatus, expectedVersion provider.Version) (newVersion provider.Version, err error) {
	mpr.Lock()
	defer mpr.Unlock()

	// During startup the FSM may still be replaying log entries while
	// the metadata layer's first Store is in flight: Get() returned
	// version N, but by the time our Apply lands the FSM is at N+k.
	// The CAS mismatch is recoverable — callers (notably
	// coordinator/metadata.persistStatusLocked) wrap Store in
	// backoff.RetryNotify expecting an *error*, not a panic. Without
	// this recover the process aborts mid-bootstrap and the cluster
	// crash-loops once per redeploy.
	defer func() {
		if r := recover(); r != nil {
			if perr, ok := r.(error); ok && stderrors.Is(perr, provider.ErrBadVersion) {
				newVersion = provider.NotExists
				err = perr
				return
			}
			panic(r)
		}
	}()

	if err = mpr.raft.VerifyLeader().Error(); err != nil {
		return provider.NotExists, err
	}

	mpr.log.Debug("Store into raft",
		slog.Any("cluster-status", cs),
		slog.Any("expected-version", expectedVersion),
		slog.Any("current-version", mpr.sc.CurrentVersion))

	// Retry up to a few times on CAS mismatch with a refreshed
	// expectedVersion taken from the FSM's actual post-replay state.
	//
	// Why: only the elected raft leader can land here (VerifyLeader
	// above), and raft serializes every Apply through the FSM, so we
	// have exclusive write access — the CAS only ever fails when log
	// replay (post-restart) advanced sc.CurrentVersion past the value
	// the caller observed via Get. Upstream's
	// coordinator/metadata.persistStatusLocked retries Store with the
	// same stale m.currentVersionID, so without internal refresh it
	// loops forever and stalls coordinator bootstrap. Refreshing once
	// per retry inside Store breaks the bootstrap deadlock without
	// undermining cross-cluster CAS protection (no concurrent writer
	// can exist; retries are bounded).
	const maxStoreRetries = 4
	requestedExpected := fromVersion(expectedVersion)
	currentExpected := requestedExpected
	for attempt := 0; ; attempt++ {
		cmd := raftOpCmd{
			NewState:        mustMarshalClusterStatus(cs),
			ExpectedVersion: currentExpected,
		}
		serializedCmd, err := json.Marshal(cmd)
		if err != nil {
			return provider.NotExists, err
		}
		future := mpr.raft.Apply(serializedCmd, 30*time.Second)
		if err := future.Error(); err != nil {
			return provider.NotExists, errors.Wrap(err, "failed to apply new cluster state")
		}
		applyRes, ok := future.Response().(*applyResult)
		if !ok {
			return provider.NotExists, errors.New("unexpected raft Apply response type")
		}
		if applyRes.changeApplied {
			return toVersion(applyRes.newVersion), nil
		}
		if attempt >= maxStoreRetries {
			return provider.NotExists, provider.ErrBadVersion
		}
		// CAS mismatch: caller's expectedVersion is stale relative to
		// our FSM. Refresh from local state and retry. Log the gap so
		// operators can spot a real concurrent-writer bug if this fires
		// outside the bootstrap window.
		newExpected := mpr.sc.CurrentVersion
		mpr.log.Info("CAS mismatch, refreshing expectedVersion from FSM and retrying",
			slog.Int64("requested-expected", requestedExpected),
			slog.Int64("previous-expected", currentExpected),
			slog.Int64("fsm-current", newExpected),
			slog.Int("attempt", attempt+1))
		currentExpected = newExpected
	}
}
