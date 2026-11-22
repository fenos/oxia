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

package election

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oxia-db/oxia/oxiad/common/async"
	"github.com/oxia-db/oxia/oxiad/common/dataserver"
	"github.com/oxia-db/oxia/oxiad/common/entry"
	"github.com/oxia-db/oxia/oxiad/common/errs"
	"github.com/oxia-db/oxia/oxiad/common/quorum"
	"github.com/oxia-db/oxia/oxiad/common/sharding"

	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"golang.org/x/exp/maps"
	gproto "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/oxiad/common/feature"
	"github.com/oxia-db/oxia/oxiad/coordinator/action"
	controllerapi "github.com/oxia-db/oxia/oxiad/coordinator/controller"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/failpoint"
	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/common/proto"
	commonrpc "github.com/oxia-db/oxia/common/rpc"
	oxiatime "github.com/oxia-db/oxia/common/time"
)

// When fencing quorum of servers, after we reach the majority, wait a bit more
// to include responses from all healthy servers.
const quorumFencingGracePeriod = 100 * time.Millisecond

var _ Election = &election{}

type election struct {
	logger        *slog.Logger
	ctx           context.Context
	ctxCancel     context.CancelFunc
	wg            sync.WaitGroup
	metadataStore coordmetadata.Metadata
	// preferredLeader is the data server that a leader transfer asks for, or
	// empty.
	preferredLeader string
	eventListener   controllerapi.ShardEventListener
	provider        rpc.ControllerRPCProvider
	namespace       string
	shard           int64
	changeEnsemble  *action.ChangeEnsembleAction
	// undoGrowth undoes a pending growth of the ensemble whose new member isn't
	// among the highest heads: the growth missed its deadline
	undoGrowth       bool
	requiredFeatures []proto.Feature
	featureSupplier  feature.Supplier
	shardMailbox     async.Mailbox

	// mutable
	meta             *proto.ShardMetadata
	started          atomic.Bool
	quorumCaughtUp   atomic.Bool
	ensembleCaughtUp atomic.Bool

	// metrics
	leaderElectionLatencyHistogram metric.LatencyHistogram
	newTermQuorumLatencyHistogram  metric.LatencyHistogram
	becomeLeaderLatencyHistogram   metric.LatencyHistogram
}

// refreshEnsembleInfo returns the ensemble and its pending change, at the
// addresses the data servers are registered with.
func (e *election) refreshEnsembleInfo(meta *proto.ShardMetadata) (ensemble []*proto.DataServerIdentity,
	change *proto.EnsembleChangeMetadata, err error) {
	servers := meta.Ensemble
	if from := meta.GetEnsembleChange().GetFrom(); from != nil {
		servers = slices.Concat(servers, []*proto.DataServerIdentity{from})
	}
	refreshed := make([]*proto.DataServerIdentity, len(servers))
	for idx, server := range servers {
		refreshed[idx] = server
		// A data server that left keeps the addresses it drains from, also
		// when the configuration has it again at other ones
		stored, _, err := e.metadataStore.GetDataServerStatus(server.GetNameOrDefault())
		if err != nil {
			return nil, nil, err
		}
		if dataserver.IsRemoved(stored.UnsafeBorrow().GetState()) {
			continue
		}
		borrowedDataServer, exist, err := e.metadataStore.GetDataServer(server.GetNameOrDefault())
		if err != nil {
			return nil, nil, err
		}
		if exist {
			if identity := borrowedDataServer.UnsafeBorrow().GetIdentity(); identity != nil {
				refreshed[idx] = identity
			}
		}
	}
	if e.logger.Enabled(e.ctx, slog.LevelDebug) {
		if !reflect.DeepEqual(servers, refreshed) {
			e.logger.Info("refresh the shard ensemble dataServer address", slog.Any("current-ensemble", servers),
				slog.Any("new-ensemble", refreshed))
		}
	}
	// Capped, so that an append to the ensemble doesn't overwrite from
	ensemble = refreshed[:len(meta.Ensemble):len(meta.Ensemble)]
	if meta.EnsembleChange != nil {
		idx := slices.IndexFunc(ensemble, dataserver.MatchDataServer(meta.EnsembleChange.To))
		if idx < 0 {
			return nil, nil, fmt.Errorf("%w: the pending change of the ensemble adds %q, which isn't a member of it",
				errs.ErrShardNotElectable, meta.EnsembleChange.To.GetNameOrDefault())
		}
		change = &proto.EnsembleChangeMetadata{To: ensemble[idx]}
		if meta.EnsembleChange.From != nil {
			change.From = refreshed[len(meta.Ensemble)]
		}
	}
	return ensemble, change, nil
}

//nolint:revive
func (e *election) fenceQuorum(term int64, options *proto.NewTermOptions, ensemble []*proto.DataServerIdentity,
	change *proto.EnsembleChangeMetadata) (ensembleHeads sharding.Candidates,
	retainedHeads sharding.Candidates, features []proto.Feature, err error) {
	fenceQuorumTimer := e.newTermQuorumLatencyHistogram.Timer()

	fencingDataServers := ensemble
	oldEnsembleSize := len(ensemble)
	if from := change.GetFrom(); from != nil {
		fencingDataServers = slices.Concat(ensemble, []*proto.DataServerIdentity{from})
	} else if change != nil {
		// A growth: the ensemble before it is the ensemble without to
		oldEnsembleSize--
	}
	fencingQuorumSize := len(fencingDataServers)
	// As Raft's joint consensus, the fence overlaps every majority of the
	// ensemble before the change, from instead of to, which holds the entries
	// committed before it, and of the ensemble, which holds the ones committed
	// since. The first takes one member less than a majority with an even
	// replication factor; the second is a majority, which the leader also needs
	// as followers. A growth that can't fence a majority of the ensemble, e.g.
	// to is down, can still be undone with a majority of the ensemble before it
	// (see settleGrowth)
	majority := quorum.Majority(len(ensemble))
	oldEnsembleQuorum := quorum.MajorityOverlap(oldEnsembleSize)
	inOldEnsemble := func(server *proto.DataServerIdentity) bool {
		return change == nil || !dataserver.MatchDataServer(change.To)(server)
	}

	// Use a new context, so we can cancel the pending requests
	fencingContext, fencingContextCancel := context.WithCancel(e.ctx)
	latch := sync.WaitGroup{}
	defer func() {
		fencingContextCancel()
		latch.Wait()
		fenceQuorumTimer.Done()
	}()

	type fenceResponse struct {
		DataServer *proto.DataServerIdentity
		Response   *proto.NewTermResponse
		Err        error
	}
	// Channel to receive responses or errors from each server
	ch := make(chan fenceResponse, fencingQuorumSize)

	for _, server := range fencingDataServers {
		latch.Go(func() {
			process.DoWithLabels(
				fencingContext,
				map[string]string{
					"oxia":        "election-fence-new-term",
					"shard":       fmt.Sprintf("%d", e.shard),
					"data-server": server.GetNameOrDefault(),
				}, func() {
					res, err := e.provider.NewTerm(fencingContext, server, &proto.NewTermRequest{
						Namespace: e.namespace,
						Shard:     e.shard,
						Term:      term,
						Options:   options,
					})
					switch {
					case errors.Is(err, constant.ErrNotInitialized):
						e.logger.Debug("FenceNewTerm is waiting for data server initialization", slog.Any("data-server", server))
					case err != nil:
						e.logger.Warn("FenceNewTerm failed", slog.Any("error", err), slog.Any("data-server", server))
					default:
						e.logger.Info("Processed fenceNewTerm response", slog.Any("data-server", server), slog.Any("entry-id", res.HeadEntryId))
					}
					ch <- fenceResponse{DataServer: server, Response: res, Err: err}
				},
			)
		})
	}
	enabledFeatures := make(map[proto.Feature]bool)
	retainedHeads = make(sharding.Candidates)
	ensembleHeads = make(sharding.Candidates)
	oldEnsembleFenced := 0
	totalResponses := 0
	for (oldEnsembleFenced < oldEnsembleQuorum || len(ensembleHeads) < majority) && totalResponses < fencingQuorumSize {
		fencingResponse := <-ch

		totalResponses++
		if fencingResponse.Err != nil {
			err = multierr.Append(err, fencingResponse.Err)
			continue
		}
		if inOldEnsemble(fencingResponse.DataServer) {
			oldEnsembleFenced++
		}
		for _, f := range fencingResponse.Response.GetFeaturesEnabled() {
			enabledFeatures[f] = true
		}
		candidate := sharding.Candidate{
			HeadEntryId:  fencingResponse.Response.HeadEntryId,
			CommitOffset: fencingResponse.Response.CommitOffset,
		}
		if slices.Contains(ensemble, fencingResponse.DataServer) {
			ensembleHeads[fencingResponse.DataServer] = candidate
		} else {
			retainedHeads[fencingResponse.DataServer] = candidate
		}
	}
	if oldEnsembleFenced < oldEnsembleQuorum {
		return nil, nil, nil, errors.Wrap(err, "election failed: quorum not reached")
	}
	if len(ensembleHeads) < majority && (!isGrowth(change) || oldEnsembleFenced < quorum.Majority(oldEnsembleSize)) {
		return nil, nil, nil, errors.Errorf(
			"election failed: quorum of the ensemble not reached: %d of %d members fenced",
			len(ensembleHeads), len(ensemble))
	}
	// If we have already reached a quorum of successful responses, we can wait a
	// tiny bit more, to allow time for all the "healthy" data servers to respond.
gracePeriod:
	for totalResponses < fencingQuorumSize {
		select {
		case r := <-ch:
			totalResponses++
			if r.Err != nil {
				// rpc has already printed the logs
				continue
			}
			for _, f := range r.Response.GetFeaturesEnabled() {
				enabledFeatures[f] = true
			}
			candidate := sharding.Candidate{HeadEntryId: r.Response.HeadEntryId, CommitOffset: r.Response.CommitOffset}
			if slices.Contains(ensemble, r.DataServer) {
				ensembleHeads[r.DataServer] = candidate
			} else {
				retainedHeads[r.DataServer] = candidate
			}
		case <-time.After(quorumFencingGracePeriod):
			break gracePeriod
		}
	}
	enabled := maps.Keys(enabledFeatures)
	slices.Sort(enabled)
	return ensembleHeads, retainedHeads, enabled, nil
}

// chooseLeader chooses a fenced data server with the highest head entry, which
// has every committed entry; ensembleHeads holds at least the fence quorum, so
// there is one:
//
//	           +----------------------+
//	heads ---->| 1. ensemble          |    the highest head entry, with its ties
//	           +----------------------+
//	                      |
//	           +----------------------+
//	           | 2. pending change    |    from strictly ahead: the change is
//	           |                      |    undone, from swaps back with to, and is
//	           |                      |    the only candidate; to tied at the
//	           |                      |    highest: the change is finished
//	           +----------------------+
//	                      |
//	           +----------------------+
//	           | 3. select            |    among the ties: the preferred leader,
//	           |                      |    else any; the others follow
//	           +----------------------+
func (e *election) chooseLeader(ensembleHeads sharding.Candidates, retainedHeads sharding.Candidates) (
	leader *proto.DataServerIdentity, followers sharding.Candidates) {
	mostUpToDate := highestHeads(ensembleHeads, retainedHeads)
	// The ensemble wins a tie: the data server removed by the change leads only
	// when it is ahead of every fenced member
	if members := slices.DeleteFunc(slices.Clone(mostUpToDate), func(server *proto.DataServerIdentity) bool {
		_, retained := retainedHeads[server]
		return retained
	}); len(members) > 0 {
		mostUpToDate = members
	}
	slices.SortStableFunc(mostUpToDate, func(a, b *proto.DataServerIdentity) int {
		return cmp.Compare(rankTie(ensembleHeads[a]), rankTie(ensembleHeads[b]))
	})
	if _, retained := retainedHeads[mostUpToDate[0]]; retained {
		e.undoEnsembleChange(mostUpToDate[0], ensembleHeads, retainedHeads)
	} else if change := e.meta.EnsembleChange; change != nil &&
		slices.ContainsFunc(mostUpToDate, dataserver.MatchDataServer(change.To)) {
		// To has every committed entry: with it, the ensemble has each of them on a
		// majority without from
		e.logger.Info("The data server added by the change of the ensemble caught up, the change is finished",
			slog.Any("from", change.From), slog.Any("to", change.To))
		e.meta.EnsembleChange = nil
		if change.From != nil {
			e.meta.PendingDeleteShardNodes = dataserver.AddDataServer(e.meta.PendingDeleteShardNodes, change.From)
		}
	}
	// The scheduler balances the leaders, so the election doesn't count them:
	// a count read by every election of a failover at once would send each to
	// the same data server. The ties come in no particular order.
	leader = mostUpToDate[0]
	if i := slices.IndexFunc(mostUpToDate, func(server *proto.DataServerIdentity) bool {
		return server.GetNameOrDefault() == e.preferredLeader
	}); i >= 0 {
		leader = mostUpToDate[i]
	}
	followers = make(sharding.Candidates)
	for server, candidate := range ensembleHeads {
		if server.GetNameOrDefault() != leader.GetNameOrDefault() {
			followers[server] = candidate
		}
	}
	return leader, followers
}

// undoEnsembleChange restores the ensemble before the pending change, as Raft
// rolls back a change that isn't committed, when the retained data server is
// ahead of every fenced member: it has every committed entry, so the ensemble
// before the change has them on a majority.
func (e *election) undoEnsembleChange(retained *proto.DataServerIdentity, ensembleHeads sharding.Candidates,
	retainedHeads sharding.Candidates) {
	change := e.meta.EnsembleChange
	e.logger.Info("The data server removed by the change of the ensemble is ahead of it, the change is undone",
		slog.Any("from", change.From), slog.Any("to", change.To))
	// To may have missed the fence
	maps.DeleteFunc(ensembleHeads, func(server *proto.DataServerIdentity, _ sharding.Candidate) bool {
		return server.GetNameOrDefault() == change.To.GetNameOrDefault()
	})
	retainedHeads.Move(retained, ensembleHeads)
	e.meta.Ensemble = append(slices.DeleteFunc(e.meta.Ensemble, dataserver.MatchDataServer(change.To)), retained)
	e.meta.EnsembleChange = nil
	e.meta.PendingDeleteShardNodes = dataserver.AddDataServer(e.meta.PendingDeleteShardNodes, change.To)
}

func (e *election) becomeLeader(term int64, leader *proto.DataServerIdentity, followers sharding.Candidates,
	replicationFactor uint32, negotiateFeatures []proto.Feature) error {
	becomeLeaderTimer := e.becomeLeaderLatencyHistogram.Timer()

	followersMap := make(map[string]*proto.EntryId)
	followerCommitOffsets := make(map[string]int64)
	for server, candidate := range followers {
		followersMap[server.GetInternal()] = candidate.HeadEntryId
		if candidate.CommitOffset != nil {
			followerCommitOffsets[server.GetInternal()] = *candidate.CommitOffset
		}
	}
	ctx, cancel := context.WithTimeout(e.ctx, commonrpc.DefaultRpcTimeout)
	defer cancel()
	if _, err := e.provider.BecomeLeader(ctx, leader, &proto.BecomeLeaderRequest{
		Namespace:             e.namespace,
		Shard:                 e.shard,
		Term:                  term,
		ReplicationFactor:     replicationFactor,
		FollowerMaps:          followersMap,
		FollowerCommitOffsets: followerCommitOffsets,
		FeaturesSupported:     negotiateFeatures,
	}); err != nil {
		return err
	}

	becomeLeaderTimer.Done()
	return nil
}

//nolint:revive
func (e *election) ensureEnsembleCaughtUp(ensemble []*proto.DataServerIdentity, leader *proto.DataServerIdentity,
	leaderEntry *proto.EntryId) error {
	ctx, cancel := context.WithCancel(e.ctx)
	waitGroup := sync.WaitGroup{}
	defer waitGroup.Wait()
	defer cancel()
	results := make(chan error, len(ensemble))
	for _, server := range ensemble {
		if server.GetNameOrDefault() == leader.GetNameOrDefault() {
			continue
		}
		waitGroup.Go(func() {
			process.DoWithLabels(
				e.ctx,
				map[string]string{
					"oxia":        "shard-caught-up-monitor",
					"namespace":   e.namespace,
					"shard":       fmt.Sprintf("%d", e.shard),
					"data-server": server.GetNameOrDefault(),
				}, func() {
					results <- backoff.RetryNotify(func() error {
						fs, err := e.provider.GetStatus(ctx, server, &proto.GetStatusRequest{Shard: e.shard})
						if err != nil {
							return err
						}
						// Unlike its head entry, which can be of a diverging log until
						// the leader truncates it, the committed entries of a follower
						// are the ones of the leader
						if fs.CommitOffset >= leaderEntry.Offset {
							e.logger.Info(
								"Follower is caught-up with the leader after election",
								slog.Any("server", server),
							)
							return nil
						}
						e.logger.Info(
							"Follower is *not* caught-up yet with the leader",
							slog.Any("server", server),
							slog.Int64("leader-head-offset", leaderEntry.Offset),
							slog.Int64("follower-commit-offset", fs.CommitOffset),
						)
						return errors.Wrap(errs.ErrFailedPrecondition, "follower not caught up yet")
					}, oxiatime.NewBackOff(ctx), func(err error, duration time.Duration) {
						switch {
						case errors.Is(err, errs.ErrFailedPrecondition):
						case errors.Is(err, constant.ErrNodeIsNotMember):
							e.logger.Info("Follower has not been added by leader yet",
								slog.Any("server", server),
								slog.Int64("shard", e.shard),
								slog.Duration("retry-after", duration),
							)
						default:
							e.logger.Warn("Failed to get the follower status",
								slog.Any("server", server),
								slog.Any("error", err),
								slog.Duration("retry-after", duration),
							)
						}
					})
				})
		})
	}
	// The leader is caught up
	for caughtUp := 1; ; caughtUp++ {
		if caughtUp == quorum.Majority(len(ensemble)) {
			e.quorumCaughtUp.Store(true)
			e.shardMailbox.Post()
		}
		if caughtUp == len(ensemble) {
			break
		}
		if err := <-results; err != nil {
			return err
		}
	}
	e.ensembleCaughtUp.Store(true)
	e.shardMailbox.Post()
	return nil
}

func (e *election) fenceNewTermAndAddFollower(ctx context.Context, term int64, options *proto.NewTermOptions,
	leader *proto.DataServerIdentity, follower *proto.DataServerIdentity) error {
	fr, err := e.provider.NewTerm(ctx, follower, &proto.NewTermRequest{
		Namespace: e.namespace,
		Shard:     e.shard,
		Term:      term,
		Options:   options,
	})
	if err != nil {
		return err
	}

	// Report the follower's supported features (from the latest handshake) so
	// the leader can refuse a joiner whose binary does not cover the features
	// required by the shard.
	followerFeatures := e.featureSupplier([]*proto.DataServerIdentity{follower})[follower.GetNameOrDefault()]

	if _, err := e.provider.AddFollower(ctx, leader, &proto.AddFollowerRequest{
		Namespace:    e.namespace,
		Shard:        e.shard,
		Term:         term,
		FollowerName: follower.GetInternal(),
		FollowerHeadEntryId: &proto.EntryId{
			Term:   fr.HeadEntryId.Term,
			Offset: fr.HeadEntryId.Offset,
		},
		FollowerFeatures:     &proto.FollowerFeatures{Supported: followerFeatures},
		FollowerCommitOffset: fr.CommitOffset,
	}); err != nil {
		return err
	}

	e.logger.Info(
		"Successfully rejoined the quorum",
		slog.Any("follower", follower),
		slog.Int64("term", fr.HeadEntryId.Term),
	)
	return nil
}

func (e *election) fencingFailedFollowers(term int64, options *proto.NewTermOptions, ensemble []*proto.DataServerIdentity,
	leader *proto.DataServerIdentity, successfulFollowers sharding.Candidates) {
	if len(successfulFollowers) == len(ensemble)-1 {
		e.logger.Debug(
			"All the member of the ensemble were successfully added",
			slog.Int64("term", term),
		)
		return
	}
	// Identify failed followers
	group := sync.WaitGroup{}
	defer group.Wait()
	for _, follower := range ensemble {
		if follower.GetNameOrDefault() == leader.GetNameOrDefault() {
			continue
		}
		if _, found := successfulFollowers[follower]; found {
			continue
		}
		e.logger.Info("Data server has failed in leader election, retrying", slog.Any("follower", follower))
		group.Go(func() {
			process.DoWithLabels(
				e.ctx,
				map[string]string{
					"oxia":     "election-retry-failed-follower",
					"shard":    fmt.Sprintf("%d", e.shard),
					"follower": follower.GetNameOrDefault(),
				},
				func() {
					bo := oxiatime.NewBackOffWithInitialInterval(e.ctx, 1*time.Second)
					_ = backoff.RetryNotify(func() error {
						err := e.fenceNewTermAndAddFollower(e.ctx, term, options, leader, follower)
						if errors.Is(err, constant.ErrInvalidTerm) {
							// If we're receiving invalid term error, it would mean
							// there's already a new term generated, and we don't have
							// to keep trying with this old term
							e.logger.Warn(
								"Failed to fenceNewTermAndAddFollower, invalid term. Stop trying",
								slog.Any("follower", follower),
								slog.Int64("term", term),
							)
							return nil
						}
						return err
					}, bo, func(err error, duration time.Duration) {
						e.logger.Warn(
							"Failed to fenceNewTermAndAddFollower, retrying later",
							slog.Any("error", err),
							slog.Any("follower", follower),
							slog.Int64("term", term),
							slog.Duration("retry-after", duration),
						)
					})
				},
			)
		})
	}
}

// prepareIfChangeEnsemble applies the change of the ensemble, unless an earlier
// election persisted it already.
func (e *election) prepareIfChangeEnsemble(mutShardMeta *proto.ShardMetadata) {
	if e.changeEnsemble == nil {
		return
	}
	from := e.changeEnsemble.From
	to := e.changeEnsemble.To
	if slices.ContainsFunc(mutShardMeta.Ensemble, dataserver.MatchDataServer(to)) {
		return
	}
	// A dataServer might get re-added to the ensemble after it was swapped out and be in
	// pending delete state. We don't want a background task to attempt deletion anymore
	mutShardMeta.PendingDeleteShardNodes = slices.DeleteFunc(mutShardMeta.PendingDeleteShardNodes,
		dataserver.MatchDataServer(to))
	if from != nil {
		mutShardMeta.Ensemble = slices.DeleteFunc(mutShardMeta.Ensemble, dataserver.MatchDataServer(from))
	}
	mutShardMeta.Ensemble = append(mutShardMeta.Ensemble, to)
	mutShardMeta.EnsembleChange = &proto.EnsembleChangeMetadata{From: from, To: to}
	e.logger.Info(
		"Changing ensemble",
		slog.Any("new-ensemble", mutShardMeta.Ensemble),
		slog.Any("from", from),
		slog.Any("to", to),
	)
}

func (e *election) newTerm() (*proto.NewTermOptions, error) {
	termOptions, err := e.metadataStore.GetNamespaceTermOptions(e.namespace)
	if err != nil {
		return nil, err
	}
	refreshedEnsemble, refreshedChange, err := e.refreshEnsembleInfo(e.meta)
	if err != nil {
		return nil, err
	}

	previousTerm := e.meta.Term
	e.meta.Status = proto.ShardStatusElection
	e.meta.Leader = nil
	e.meta.Term++
	e.meta.EnsembleCaughtUp = false
	e.meta.Ensemble = refreshedEnsemble
	e.meta.EnsembleChange = refreshedChange
	if err = e.metadataStore.PatchShardStatus(e.namespace, e.shard, func(current *proto.ShardMetadata) (bool, error) {
		switch {
		case current.GetStatusOrDefault() == proto.ShardStatusDeleting:
			return false, fmt.Errorf("%w: the shard is being deleted", errs.ErrShardNotElectable)
		case len(current.GetSplit().GetChildShardIds()) > 0 &&
			current.GetSplit().GetPhaseOrDefault() == proto.SplitPhaseFinalize:
			return false, fmt.Errorf("%w: the shard is the parent of a split past the point of no return",
				errs.ErrShardNotElectable)
		case e.changeEnsemble != nil && current.Split != nil:
			return false, fmt.Errorf("%w: the shard is part of a split", errs.ErrShardNotElectable)
		case current.GetTerm() != previousTerm:
			return false, termMovedError(previousTerm, current.GetTerm())
		}
		current.Status = e.meta.Status
		current.Term = e.meta.Term
		current.EnsembleCaughtUp = e.meta.EnsembleCaughtUp
		current.Leader = e.meta.Leader
		current.Ensemble = e.meta.Ensemble
		current.EnsembleChange = e.meta.EnsembleChange
		current.PendingDeleteShardNodes = e.meta.PendingDeleteShardNodes
		return true, nil
	}); err != nil {
		return nil, errors.Wrap(err, "failed to persist the new term")
	}
	return termOptions, nil
}

func (e *election) Start() (candidateLeader *proto.DataServerIdentity, err error) {
	if !e.started.CompareAndSwap(false, true) {
		return nil, fmt.Errorf("%w: the election has started already", errs.ErrFailedPrecondition)
	}
	e.logger.Info("Starting a new election")
	timer := e.leaderElectionLatencyHistogram.Timer()

	// A growth is recorded with the new term, so that the data server it adds
	// is a member, and out of the pending deletions, before any fence: a
	// coordinator that reads the status from now on never deletes its copy. A
	// swap is recorded once fenced, as it removes a member
	growth := e.changeEnsemble != nil && e.changeEnsemble.From == nil
	if growth {
		e.prepareIfChangeEnsemble(e.meta)
	}
	termOptions, err := e.newTerm()
	if err != nil {
		return nil, err
	}
	if err := failpoint.Step(e.ctx, "election.afterNewTerm"); err != nil {
		return nil, err
	}

	if !growth {
		e.prepareIfChangeEnsemble(e.meta)
	}

	// Negotiate the feature set across the new ensemble before fencing, so it
	// can be pinned in the term options: every member persists it with the
	// term and rejects the fence if its binary does not support it. The
	// features that an earlier attempt found already enabled on the shard are
	// pinned as well (see checkNegotiatedFeatures).
	features := e.featureSupplier(e.meta.Ensemble)
	negotiatedFeatures := feature.Union(feature.Negotiate(features, len(e.meta.Ensemble)), e.requiredFeatures)
	termOptions.Features = negotiatedFeatures

	ensembleHeads, retainedHeads, err := e.fence(termOptions, negotiatedFeatures)
	if err != nil {
		return nil, err
	}
	candidateLeader, followers := e.chooseLeader(ensembleHeads, retainedHeads)
	e.logger.Info(
		"Successfully moved ensemble to a new term",
		slog.Int64("term", e.meta.Term),
		slog.Any("new-leader", candidateLeader),
		slog.Any("followers", followers),
	)
	leaderHead := ensembleHeads[candidateLeader].HeadEntryId
	// Persisted first: if BecomeLeader fails, the next election keeps it
	e.meta.Status = proto.ShardStatusSteadyState
	e.meta.Leader = candidateLeader

	if err = e.metadataStore.PatchShardStatus(e.namespace, e.shard, func(current *proto.ShardMetadata) (bool, error) {
		if current.GetTerm() != e.meta.Term {
			return false, termMovedError(e.meta.Term, current.GetTerm())
		}
		if current.GetStatusOrDefault() != proto.ShardStatusDeleting {
			current.Status = e.meta.Status
		}
		current.Term = e.meta.Term
		current.Leader = e.meta.Leader
		current.Ensemble = e.meta.Ensemble
		current.EnsembleChange = e.meta.EnsembleChange
		current.PendingDeleteShardNodes = e.meta.PendingDeleteShardNodes
		return true, nil
	}); err != nil {
		return nil, errors.Wrap(err, "failed to persist the new leader")
	}
	if err := failpoint.Step(e.ctx, "election.afterLeaderStored"); err != nil {
		return nil, err
	}
	if err = e.becomeLeader(e.meta.Term, candidateLeader, followers, uint32(len(e.meta.Ensemble)),
		negotiatedFeatures); err != nil {
		return nil, err
	}
	if err := failpoint.Step(e.ctx, "election.afterBecomeLeader"); err != nil {
		return nil, err
	}
	if e.eventListener != nil {
		e.eventListener.LeaderElected(e.shard, candidateLeader, maps.Keys(followers))
	}

	e.logger.Info(
		"Elected new leader",
		slog.Int64("term", e.meta.Term),
		slog.Any("leader", e.meta.Leader),
		slog.Any("ensemble", e.meta.Ensemble),
	)
	timer.Done()

	e.wg.Go(func() {
		process.DoWithLabels(
			e.ctx,
			map[string]string{
				"oxia":  "election-fencing-failed-followers",
				"shard": fmt.Sprintf("%d", e.shard),
			}, func() {
				e.fencingFailedFollowers(e.meta.Term, termOptions, e.meta.Ensemble, candidateLeader, followers)
			},
		)
	})
	e.wg.Go(func() {
		process.DoWithLabels(
			e.ctx,
			map[string]string{
				"oxia":  "election-monitor-followers-caught-up",
				"shard": fmt.Sprintf("%d", e.shard),
			}, func() {
				if err := e.ensureEnsembleCaughtUp(e.meta.Ensemble, candidateLeader, leaderHead); err != nil {
					e.logger.Info("Stopped waiting for the followers to catch up", slog.Any("error", err))
				}
			},
		)
	})
	return candidateLeader, nil
}

// fence sends NewTerm to the members of the ensemble, at the term of the
// election with termOptions, until a quorum is fenced, and settles a pending
// growth of the ensemble from their heads (see settleGrowth). It fails when a
// log of the ensemble holds entries of the term already, and moves the next
// election above it.
func (e *election) fence(termOptions *proto.NewTermOptions, negotiatedFeatures []proto.Feature) (
	ensembleHeads sharding.Candidates, retainedHeads sharding.Candidates, err error) {
	ensembleHeads, retainedHeads, enabledFeatures, err := e.fenceQuorum(
		e.meta.Term,
		termOptions,
		e.meta.Ensemble,
		e.meta.EnsembleChange)
	if err != nil {
		return nil, nil, e.moveToReportedTerm(err)
	}
	if err := failpoint.Step(e.ctx, "election.afterFence"); err != nil {
		return nil, nil, err
	}
	if err = e.checkNegotiatedFeatures(negotiatedFeatures, enabledFeatures, ensembleHeads); err != nil {
		return nil, nil, err
	}
	if written := highestWrittenTerm(ensembleHeads, retainedHeads); written >= e.meta.Term {
		// A log holds entries of this term already: a leadership at it
		// happened that the shard status doesn't know of, e.g. the status was
		// restored from a backup, or a standalone data directory is adopted.
		// Leading again under a term that was written to would lead the shard
		// twice under one term, so the next election fences above it
		e.logger.Info("The term of the election was already written to, moving above it",
			slog.Int64("term", e.meta.Term), slog.Int64("written-term", written))
		if err := e.moveTerm(written); err != nil {
			return nil, nil, multierr.Append(errTermAlreadyWritten, err)
		}
		return nil, nil, errTermAlreadyWritten
	}
	if err = e.settleGrowth(ensembleHeads); err != nil {
		return nil, nil, err
	}
	return ensembleHeads, retainedHeads, nil
}

// unknownTerm is the term of the head that a data server reports when its WAL
// holds no entry: the commit offset of its database, e.g. once it installed a
// snapshot, whose entry's term it doesn't know.
const unknownTerm = -1

// isSnapshotHead reports whether head is the commit offset of a database
// with an empty WAL, rather than the last entry of a WAL.
func isSnapshotHead(head *proto.EntryId) bool {
	return head.GetTerm() == unknownTerm && head.GetOffset() >= 0
}

// highestHeads returns the fenced data servers with the most up-to-date
// heads: the ones that hold every committed entry. It takes the maximum of
// each kind of head on its own, then compares the two, so that the result
// doesn't depend on the order in which the data servers answered the fence:
//
//  1. Heads of WAL entries rank by term, then by offset, as entry.Compare
//     does and as upstream ranks every head: an entry of a higher term
//     overrides one of a lower term at the same offset, so the highest holds
//     every entry the others could have committed.
//  2. A data server whose WAL is empty, e.g. one seeded by a snapshot with
//     nothing appended since, reports the commit offset of its database with
//     an unknown term. Its database applies only committed entries, and a
//     committed entry is never overwritten, so its head holds exactly the
//     committed entries up to its offset. These heads rank by offset.
//  3. The highest of each kind rank by offset. A snapshot at a higher offset
//     than the highest WAL head holds committed entries that WAL lacks; at a
//     lower offset, the WAL may hold committed entries beyond it; at the same
//     offset both hold every committed entry, and both are the highest.
//
// The choice is so upstream's unless the highest WAL head lacks committed
// entries that a snapshot holds. Ranking every head term first instead, a
// snapshot's head always loses, and an idle shard can't elect a member seeded
// by a snapshot over its old leader. Ranking a snapshot's head against each
// WAL head by offset, in one running maximum, isn't transitive: a snapshot
// that ties the highest WAL head would make way for a longer WAL of an older
// term, whose entries could differ from the committed ones.
//
// A WAL of an older term longer than every snapshot still wins over them, as
// upstream: only the term of a snapshot's last entry would tell whether that
// WAL's entries up to the snapshot's offset are the committed ones.
func highestHeads(candidateSets ...sharding.Candidates) []*proto.DataServerIdentity {
	var heads headMax
	for _, candidates := range candidateSets {
		for server, candidate := range candidates {
			heads.offer(server, candidate.HeadEntryId)
		}
	}
	return heads.highest()
}

// headMax folds the fenced heads, in any order, into the highest ones (see
// highestHeads).
type headMax struct {
	wal      highest
	snapshot highest
}

func (m *headMax) offer(server *proto.DataServerIdentity, head *proto.EntryId) {
	if isSnapshotHead(head) {
		m.snapshot.offer(server, head, compareOffsets)
	} else {
		m.wal.offer(server, head, entry.Compare)
	}
}

func (m *headMax) highest() []*proto.DataServerIdentity {
	if m.snapshot.head == nil {
		return m.wal.servers
	}
	if m.wal.head == nil {
		return m.snapshot.servers
	}
	switch compareOffsets(m.snapshot.head, m.wal.head) {
	case 1:
		return m.snapshot.servers
	case -1:
		return m.wal.servers
	default:
		return append(slices.Clone(m.wal.servers), m.snapshot.servers...)
	}
}

// highest is the highest head offered, by an order of heads of one kind, and
// the data servers at it.
type highest struct {
	head    *proto.EntryId
	servers []*proto.DataServerIdentity
}

func (h *highest) offer(server *proto.DataServerIdentity, head *proto.EntryId,
	compare func(a *proto.EntryId, b *proto.EntryId) int) {
	if h.head == nil || compare(head, h.head) > 0 {
		h.head, h.servers = head, []*proto.DataServerIdentity{server}
	} else if compare(head, h.head) == 0 {
		h.servers = append(h.servers, server)
	}
}

func compareOffsets(a *proto.EntryId, b *proto.EntryId) int {
	return cmp.Compare(a.GetOffset(), b.GetOffset())
}

// rankTie orders the candidates tied at the highest head, the first to lead.
// A head of a snapshot ties with the head of a WAL entry at the same offset
// (see highestHeads), but they aren't as good a leader:
//   - a WAL whose entries are all committed, its commit offset at its head,
//     goes first: it holds every committed entry, and as the leader it can
//     match the entries of its followers against its own;
//   - a snapshot goes next: it holds every committed entry too, but its
//     followers with WAL entries start over from a snapshot of it, as it can't
//     match their entries;
//   - a WAL whose head isn't known to be committed goes last: its last
//     entries could be of a stale term that a committed entry at the same
//     offset replaced.
func rankTie(candidate sharding.Candidate) int {
	switch head := candidate.HeadEntryId; {
	case isSnapshotHead(head):
		return 1
	case candidate.CommitOffset != nil && *candidate.CommitOffset >= head.GetOffset():
		return 0
	default:
		return 2
	}
}

// isGrowth reports whether change adds a member to the ensemble without
// removing one.
func isGrowth(change *proto.EnsembleChangeMetadata) bool {
	return change != nil && change.From == nil
}

// errTermMoved fails a write of the election whose shard moved to another term
// since the election read it, e.g. elected by another coordinator: the next
// election starts from the stored status.
var errTermMoved = fmt.Errorf("%w: the term of the shard moved", errs.ErrUnavailable)

func termMovedError(expected int64, current int64) error {
	return fmt.Errorf("%w: from %d to %d", errTermMoved, expected, current)
}

// errGrowthNotUndone fails an election that has to undo a pending growth, but
// fenced less than a majority of the ensemble before it: the next election
// tries again.
var errGrowthNotUndone = fmt.Errorf("%w: the growth of the ensemble can't be undone yet", errs.ErrUnavailable)

// settleGrowth decides what the election does with a pending growth of the
// ensemble, from the fenced heads:
//   - to is among the highest heads: it has every committed entry, so the
//     growth is finished;
//   - the fence didn't reach a majority of the ensemble, e.g. to is down, or the
//     growth missed its deadline (see undoGrowth): it is undone, as Raft rolls
//     back a change that isn't committed. Every entry committed while the
//     growth was pending is on a majority of the ensemble, so on a majority
//     less one of the members before the growth, which overlaps any majority
//     of them: with a majority of them fenced, the highest head has every
//     committed entry. To goes to the pending deletions;
//   - otherwise, the growth stays pending, and to follows.
func (e *election) settleGrowth(ensembleHeads sharding.Candidates) error {
	change := e.meta.EnsembleChange
	if !isGrowth(change) {
		return nil
	}
	isTo := dataserver.MatchDataServer(change.To)
	if slices.ContainsFunc(highestHeads(ensembleHeads), isTo) {
		e.logger.Info("The data server added by the growth of the ensemble caught up, the growth is finished",
			slog.Any("to", change.To))
		e.meta.EnsembleChange = nil
		return nil
	}
	if len(ensembleHeads) >= quorum.Majority(len(e.meta.Ensemble)) && !e.undoGrowth {
		return nil
	}
	fencedBefore := 0
	for server := range ensembleHeads {
		if !isTo(server) {
			fencedBefore++
		}
	}
	if fencedBefore < quorum.Majority(len(e.meta.Ensemble)-1) {
		return errGrowthNotUndone
	}
	e.logger.Info("The growth of the ensemble is undone", slog.Any("to", change.To),
		slog.Bool("deadline-missed", e.undoGrowth))
	maps.DeleteFunc(ensembleHeads, func(server *proto.DataServerIdentity, _ sharding.Candidate) bool {
		return isTo(server)
	})
	e.meta.Ensemble = slices.DeleteFunc(e.meta.Ensemble, isTo)
	e.meta.EnsembleChange = nil
	e.meta.PendingDeleteShardNodes = dataserver.AddDataServer(e.meta.PendingDeleteShardNodes, change.To)
	return nil
}

// errTermAlreadyWritten ends an election whose term a log of the ensemble
// already holds entries of: the next election fences above that term.
var errTermAlreadyWritten = errors.New("election failed: the term was already written to")

// moveToReportedTerm moves the term of the shard to the highest term that a
// data server rejecting the fence reported, when it is ahead of the election:
// the next election fences one above it instead of counting up one term per
// attempt. It returns the error of the fence, with the error of the move.
func (e *election) moveToReportedTerm(fenceErr error) error {
	reported, ok := highestTermHint(fenceErr)
	if !ok || reported <= e.meta.Term {
		return fenceErr
	}
	e.logger.Info("A data server is at a later term than the election, moving to it",
		slog.Int64("term", e.meta.Term), slog.Int64("reported-term", reported))
	return multierr.Append(fenceErr, e.moveTerm(reported))
}

// moveTerm stores term as the term of the shard, unless it has a later one
// already, so that the next election starts above it.
func (e *election) moveTerm(term int64) error {
	if err := e.metadataStore.PatchShardStatus(e.namespace, e.shard, func(current *proto.ShardMetadata) (bool, error) {
		if current.GetTerm() >= term {
			return false, nil
		}
		current.Term = term
		return true, nil
	}); err != nil {
		return errors.Wrap(err, "failed to persist the term to move to")
	}
	return nil
}

// highestTermHint finds the highest term that a data server rejecting the
// fence reported in err, which joins the errors of the members.
func highestTermHint(err error) (int64, bool) {
	var (
		highest int64
		found   bool
	)
	var walk func(error)
	walk = func(err error) {
		var invalidTerm constant.InvalidTermError
		if errors.As(err, &invalidTerm) && (!found || invalidTerm.Current > highest) {
			highest, found = invalidTerm.Current, true
		}
		switch unwrapped := err.(type) { //nolint:errorlint // walking the tree of errors, not matching one
		case interface{ Unwrap() []error }:
			for _, inner := range unwrapped.Unwrap() {
				walk(inner)
			}
		case interface{ Unwrap() error }:
			walk(unwrapped.Unwrap())
		default:
			// A leaf: nothing beneath it
		}
	}
	walk(err)
	return highest, found
}

// highestWrittenTerm is the highest term of the head entries of the fenced
// data servers, or -1 when every log is empty.
func highestWrittenTerm(heads ...sharding.Candidates) int64 {
	highest := constant.I64NegativeOne
	for _, candidates := range heads {
		for _, candidate := range candidates {
			if head := candidate.HeadEntryId; head.GetOffset() >= 0 && head.GetTerm() > highest {
				highest = head.GetTerm()
			}
		}
	}
	return highest
}

// checkNegotiatedFeatures validates the feature set pinned by the new term
// once the ensemble is fenced. The members' features are read again: they are
// only known after the member's handshake with this coordinator, which can
// complete while an election is running (e.g. at coordinator startup), and
// until then the member counts as not supporting any feature.
//
// todo: move the check to the feature package, and share it with the split
// controller, which makes the same check for the children of a split.
func (e *election) checkNegotiatedFeatures(negotiated []proto.Feature, enabled []proto.Feature,
	candidatesStatus sharding.Candidates) error {
	ensemble := e.meta.Ensemble
	features := e.featureSupplier(ensemble)

	// The features pinned by the term, and the ones already enabled on the
	// shard, must be supported by every member that takes part in the term,
	// or the unsupported members would apply entries with different semantics
	// and silently diverge. Fail the election instead: the shard stays
	// unavailable until the offending nodes are replaced.
	participantsFeatures := make(map[string][]proto.Feature, len(candidatesStatus))
	for dataServer := range candidatesStatus {
		participantsFeatures[dataServer.GetNameOrDefault()] = features[dataServer.GetNameOrDefault()]
	}
	required := feature.Union(negotiated, enabled)
	if missing := feature.Missing(required, feature.Negotiate(participantsFeatures, len(participantsFeatures))); len(missing) > 0 {
		e.logger.Error(
			"Election aborted: the ensemble does not support features already enabled on the shard",
			slog.Int64("term", e.meta.Term),
			slog.Any("missing-features", missing),
			slog.Any("negotiated-features", negotiated),
			slog.Any("data-server-features", participantsFeatures),
		)
		return errors.Wrapf(constant.ErrUnsupportedFeatures,
			"ensemble does not support features %v already enabled on shard %d", missing, e.shard)
	}

	// The members that did not take part, e.g. because they are down, are
	// checked when they join the term (see fenceNewTermAndAddFollower), and a
	// feature enabled on the shard can never be disabled again: pin the
	// enabled features even if those members' support is unknown, instead of
	// failing every election until they are back.
	if missing := feature.Missing(enabled, negotiated); len(missing) > 0 {
		return &feature.EnabledError{Features: enabled}
	}

	// A handshake completed while fencing the ensemble: pin the features that
	// the whole ensemble supports now, or they would stay disabled until the
	// next election.
	if added := feature.Missing(feature.Negotiate(features, len(ensemble)), negotiated); len(added) > 0 {
		return fmt.Errorf("%w: the ensemble now supports features %v", errs.ErrShardFeaturesChanged, added)
	}
	return nil
}

func (e *election) Tick() error {
	// The whole ensemble caught up after its majority did
	ensembleCaughtUp := e.ensembleCaughtUp.Load()
	finishChange := e.meta.EnsembleChange != nil && (ensembleCaughtUp || e.quorumCaughtUp.Load())
	markCaughtUp := ensembleCaughtUp && !e.meta.EnsembleCaughtUp
	if !finishChange && !markCaughtUp {
		return nil
	}
	var finished *proto.EnsembleChangeMetadata
	if err := e.metadataStore.PatchShardStatus(e.namespace, e.shard, func(current *proto.ShardMetadata) (bool, error) {
		// During a handover, the other coordinator may have elected since,
		// undone the change, or started another
		if current.Term != e.meta.Term {
			return false, nil
		}
		if finishChange && current.EnsembleChange != nil {
			finished = current.EnsembleChange
			if finished.From != nil {
				current.PendingDeleteShardNodes = dataserver.AddDataServer(current.PendingDeleteShardNodes, finished.From)
			}
			current.EnsembleChange = nil
		}
		current.EnsembleCaughtUp = current.EnsembleCaughtUp || markCaughtUp
		return true, nil
	}); err != nil {
		return err
	}
	if finished != nil {
		e.logger.Info("A majority of the ensemble caught up, the change of the ensemble is finished",
			slog.Any("from", finished.From), slog.Any("to", finished.To))
	}
	// Done, or left to the election of a later term
	if finishChange {
		e.meta.EnsembleChange = nil
	}
	e.meta.EnsembleCaughtUp = e.meta.EnsembleCaughtUp || markCaughtUp
	return nil
}

func (e *election) IsQuorumCaughtUp() bool {
	return e.quorumCaughtUp.Load()
}

func (e *election) Cancel() {
	e.ctxCancel()
	e.wg.Wait()
	e.logger.Info("stopped the election", slog.Any("term", e.meta.GetTerm()))
}

// New returns an election of the shard, that elects preferredLeader if it has
// every committed entry, unless it is empty. requiredFeatures are pinned in its
// term (see feature.EnabledError).
//
//nolint:revive
func New(ctx context.Context, logger *slog.Logger, eventListener controllerapi.ShardEventListener,
	shardMailbox async.Mailbox,
	metadataStore coordmetadata.Metadata,
	featureSupplier feature.Supplier,
	provider rpc.ControllerRPCProvider, namespace string, shard int64,
	shardMetadata *proto.ShardMetadata,
	changeEnsembleAction *action.ChangeEnsembleAction, preferredLeader string, undoGrowth bool,
	requiredFeatures []proto.Feature,
	leaderElectionLatencyHistogram metric.LatencyHistogram, newTermQuorumLatencyHistogram metric.LatencyHistogram,
	becomeLeaderLatencyHistogram metric.LatencyHistogram) Election {
	current, cancelFunc := context.WithCancel(ctx)
	return &election{
		logger:                         logger,
		ctx:                            current,
		ctxCancel:                      cancelFunc,
		wg:                             sync.WaitGroup{},
		metadataStore:                  metadataStore,
		featureSupplier:                featureSupplier,
		eventListener:                  eventListener,
		shardMailbox:                   shardMailbox,
		preferredLeader:                preferredLeader,
		provider:                       provider,
		namespace:                      namespace,
		shard:                          shard,
		meta:                           gproto.CloneOf(shardMetadata),
		changeEnsemble:                 changeEnsembleAction,
		undoGrowth:                     undoGrowth,
		requiredFeatures:               requiredFeatures,
		leaderElectionLatencyHistogram: leaderElectionLatencyHistogram,
		newTermQuorumLatencyHistogram:  newTermQuorumLatencyHistogram,
		becomeLeaderLatencyHistogram:   becomeLeaderLatencyHistogram,
	}
}
