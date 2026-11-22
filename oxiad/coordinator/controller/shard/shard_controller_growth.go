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
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/pkg/errors"

	"github.com/oxia-db/oxia/common/channel"
	"github.com/oxia-db/oxia/common/failpoint"
	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/dataserver"
	"github.com/oxia-db/oxia/oxiad/common/errs"
	featurepkg "github.com/oxia-db/oxia/oxiad/common/feature"
	"github.com/oxia-db/oxia/oxiad/coordinator/action"
)

// This file owns the growth of the ensemble: a change of the ensemble that adds
// a member without removing one, when the replication factor of the namespace
// was raised. shard_controller_default.go owns the other changes.
//
// A growth runs in three steps:
//
//  1. Catch-up: the copy of the new member is recorded among the pending
//     deletions, so that a growth abandoned, also by a coordinator that stops,
//     leaves no copy behind. The new member is fenced at the current term,
//     and follows the leader as an observer, which counts for nothing, until
//     its head is within GrowthCatchUpLag entries of the commit offset of the
//     leader. The controller's event loop keeps running meanwhile, and keeps
//     the copy. The growth gives up if the new member was removed, or the
//     shard split, meanwhile.
//  2. Election: the growth is recorded with the new term, the new member
//     included and out of the pending deletions, and the election fences the
//     ensemble before and after the growth (see the election package).
//  3. Finish: the growth stays pending until the election finishes it, or
//     the leader's tick once a majority of the ensemble caught up. A growth
//     still pending after GrowthFinishTimeout, or whose new member fails, is
//     settled by an election, which undoes it if the new member isn't among
//     the highest heads.
//
// A growth onto a data server that failed, its catch-up past its deadline or
// the growth undone, backs off before the next growth onto it: by
// DefaultGrowthBackoff, doubled at each failure in a row up to
// DefaultGrowthBackoffCap. A growth finished onto it clears its failures.

const (
	// GrowthCatchUpLag is how many entries the new member of a growth can be
	// behind the commit offset of the leader when the growth is recorded:
	// once it is, the leader commits the writes of a two-member ensemble only
	// with the new member, so the writes stall until it caught up. A thousand
	// entries take well under a second to replicate at the write rates of a
	// shard.
	GrowthCatchUpLag = 1000
	// DefaultGrowthCatchUpTimeout is how long the new member of a growth can
	// take to catch up as an observer: a large shard is copied as a snapshot
	// first.
	DefaultGrowthCatchUpTimeout = 10 * time.Minute
	// DefaultGrowthFinishTimeout is how long a growth can stay pending before
	// an election settles it.
	DefaultGrowthFinishTimeout = 2 * time.Minute
	// DefaultGrowthBackoff is how long the growth onto a data server waits
	// after one failed.
	DefaultGrowthBackoff = time.Minute
	// DefaultGrowthBackoffCap bounds the wait of the growth onto a data server
	// whose growths failed in a row.
	DefaultGrowthBackoffCap = 30 * time.Minute
	// growthCatchUpPollInterval is how often the catch-up compares the new
	// member with the leader.
	growthCatchUpPollInterval = 200 * time.Millisecond
)

// errCatchUpTimedOut ends the catch-up of a new member that didn't get within
// GrowthCatchUpLag entries of the leader before its deadline.
var errCatchUpTimedOut = errors.Wrap(errs.ErrUnavailable, "the new member didn't catch up in time")

// Option configures a shard controller.
type Option func(*controller)

// WithGrowthTimeouts sets how long the new member of a growth can take to catch
// up as an observer, and how long the growth can stay pending; zero keeps the
// default.
func WithGrowthTimeouts(catchUp time.Duration, finish time.Duration) Option {
	return func(s *controller) {
		if catchUp > 0 {
			s.growthCatchUpTimeout = catchUp
		}
		if finish > 0 {
			s.growthFinishTimeout = finish
		}
	}
}

// WithGrowthBackoff sets how long the growth onto a data server waits after
// one failed, and the bound of the wait after failures in a row; zero keeps
// the default.
func WithGrowthBackoff(backoff time.Duration, backoffCap time.Duration) Option {
	return func(s *controller) {
		if backoff > 0 {
			s.growthBackoff = backoff
		}
		if backoffCap > 0 {
			s.growthBackoffCap = backoffCap
		}
	}
}

// growthCatchUp is the outcome of the catch-up of the new member of a growth,
// for the event loop.
type growthCatchUp struct {
	action *action.ChangeEnsembleAction
	// leader and term are the leader and the term of the shard that the
	// catch-up followed
	leader *proto.DataServerIdentity
	term   int64
	err    error
}

// growthBackoff is the failures in a row of the growths onto a data server,
// and until when the next one waits.
type growthBackoff struct {
	failures int
	until    time.Time
}

// isGrowth reports whether the pending change of the shard is a growth.
func isGrowth(shardMeta *proto.ShardMetadata) bool {
	change := shardMeta.GetEnsembleChange()
	return change != nil && change.From == nil
}

// startGrowth starts the catch-up of the new member of a growth, which reports
// to the event loop. It is invoked from the event loop, once the change is
// validated.
func (s *controller) startGrowth(shardMeta *proto.ShardMetadata, ac *action.ChangeEnsembleAction) {
	if shardMeta.Leader == nil || shardMeta.GetStatusOrDefault() != proto.ShardStatusSteadyState {
		ac.Error(errors.Wrap(errs.ErrUnavailable, "the shard has no leader to grow its ensemble from"))
		return
	}
	if until, backingOff := s.growthBackingOff(ac.To); backingOff {
		ac.Error(fmt.Errorf("%w: the growth onto data server %q backs off until %s", errs.ErrFailedPrecondition,
			ac.To.GetNameOrDefault(), until.Format(time.RFC3339)))
		return
	}
	termOptions, err := s.metadataStore.GetNamespaceTermOptions(s.namespace)
	if err != nil {
		ac.Error(err)
		return
	}
	if err := s.addPendingDelete(ac.To); err != nil {
		ac.Error(errors.Wrap(err, "record the copy of the new member to delete"))
		return
	}
	leader, term := shardMeta.Leader, shardMeta.Term
	s.catchingUp = ac.To
	s.logger.Info("Catching up the new member of the ensemble", slog.Any("to", ac.To), slog.Int64("term", term))
	s.wg.Go(func() {
		process.DoWithLabels(s.ctx, map[string]string{
			"oxia":      "shard-controller-growth",
			"namespace": s.namespace,
			"shard":     fmt.Sprintf("%d", s.shard),
		}, func() {
			ctx, cancel := context.WithTimeout(s.ctx, s.growthCatchUpTimeout)
			defer cancel()
			err := s.catchUpGrowth(ctx, leader, term, termOptions, ac.To)
			// The event loop is the only reader, and handles one catch-up at
			// a time: the slot is always free
			channel.PushNoBlock(s.growthCatchUpOp, growthCatchUp{action: ac, leader: leader, term: term, err: err})
		})
	})
}

// catchUpGrowth fences to at term, has the leader stream the shard to it as an
// observer, and waits until its head is within GrowthCatchUpLag entries of the
// commit offset of the leader, read at each round: the leader keeps writing.
// It removes the observer when it gives up.
func (s *controller) catchUpGrowth(ctx context.Context, leader *proto.DataServerIdentity, term int64,
	termOptions *proto.NewTermOptions, to *proto.DataServerIdentity) (err error) {
	fenced, err := s.rpc.NewTerm(ctx, to, &proto.NewTermRequest{
		Namespace: s.namespace,
		Shard:     s.shard,
		Term:      term,
		Options:   termOptions,
	})
	if err != nil {
		return errors.Wrap(err, "fence the new member at the current term")
	}
	features := featurepkg.Negotiate(s.featureSupplier([]*proto.DataServerIdentity{to}), 1)
	if _, err = s.rpc.AddFollower(ctx, leader, &proto.AddFollowerRequest{
		Namespace:           s.namespace,
		Shard:               s.shard,
		Term:                term,
		FollowerName:        to.GetInternal(),
		FollowerHeadEntryId: fenced.GetHeadEntryId(),
		Observer:            true,
		FollowerFeatures:    &proto.FollowerFeatures{Supported: features},
	}); err != nil {
		return errors.Wrap(err, "add the new member as an observer")
	}
	defer func() { //nolint:contextcheck // the removal runs past the deadline of the catch-up context
		if err != nil {
			s.removeGrowthObserver(leader, term, to)
		}
	}()
	ticker := time.NewTicker(growthCatchUpPollInterval)
	defer ticker.Stop()
	for {
		caughtUp, err := s.growthCaughtUp(ctx, leader, term, to)
		if err != nil {
			return err
		}
		if caughtUp {
			return failpoint.Step(ctx, "growth.afterCatchUp")
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return errCatchUpTimedOut
		}
	}
}

// growthCaughtUp reports whether the head of to is within GrowthCatchUpLag
// entries of the commit offset of the leader, which must still lead at term.
func (s *controller) growthCaughtUp(ctx context.Context, leader *proto.DataServerIdentity, term int64,
	to *proto.DataServerIdentity) (bool, error) {
	leaderStatus, err := s.rpc.GetStatus(ctx, leader, &proto.GetStatusRequest{Shard: s.shard})
	if err != nil {
		// Retried at the next round, until the deadline
		return false, nil //nolint:nilerr
	}
	if leaderStatus.Term != term || leaderStatus.Status != proto.ServingStatus_LEADER {
		return false, errors.Wrapf(errs.ErrUnavailable, "the leader of term %d changed", term)
	}
	toStatus, err := s.rpc.GetStatus(ctx, to, &proto.GetStatusRequest{Shard: s.shard})
	if err != nil {
		return false, nil //nolint:nilerr
	}
	return leaderStatus.CommitOffset-toStatus.HeadOffset <= GrowthCatchUpLag, nil
}

// onGrowthCaughtUp runs the election that records the growth, once its new
// member caught up, unless the shard was elected since, as the observer then
// followed a leader that is gone, the new member was removed, or the shard
// split, meanwhile. A growth that doesn't run leaves the copy of its new
// member to the pending deletions, and one whose catch-up missed its deadline
// backs off.
func (s *controller) onGrowthCaughtUp(result growthCatchUp) {
	s.catchingUp = nil
	ac := result.action
	err := result.err
	var shardMeta *proto.ShardMetadata
	if err == nil {
		shardMeta, err = s.loadShardMeta()
	}
	if err == nil && (shardMeta.Term != result.term || shardMeta.GetEnsembleChange() != nil) {
		err = errors.Wrapf(errs.ErrUnavailable, "the shard moved past term %d while its new member caught up", result.term)
	}
	if err == nil {
		err = s.refuseRemovedTarget(ac.To)
	}
	if err == nil {
		err = s.validateChangeEnsembleFeatures(shardMeta, ac)
	}
	if err != nil {
		s.logger.Warn("The growth of the ensemble gave up, its new member's copy is to delete", slog.Any("to", ac.To),
			slog.Any("error", err))
		if result.err == nil {
			// The catch-up left the leader streaming to the observer
			s.wg.Go(func() { s.removeGrowthObserver(result.leader, result.term, ac.To) })
		}
		if errors.Is(result.err, errCatchUpTimedOut) {
			s.backOffGrowth(ac.To)
		}
		ac.Error(err)
		return
	}
	s.runChange(shardMeta, ac)
}

// removeGrowthObserver removes the observer of a growth that gave up from the
// leader it followed, best effort: the next term of the leader drops it
// anyway. It runs with the controller's context, which outlives the deadline
// of the catch-up.
func (s *controller) removeGrowthObserver(leader *proto.DataServerIdentity, term int64, to *proto.DataServerIdentity) {
	ctx, cancel := context.WithTimeout(s.ctx, time.Second)
	defer cancel()
	if _, err := s.rpc.RemoveObserver(ctx, leader, &proto.RemoveObserverRequest{
		Namespace:    s.namespace,
		Shard:        s.shard,
		Term:         term,
		FollowerName: to.GetInternal(),
		TargetShard:  s.shard,
	}); err != nil {
		s.logger.Warn("Failed to remove the observer of a growth that gave up", slog.Any("to", to),
			slog.Any("error", err))
	}
}

// addPendingDelete adds the data server to the pending deletions of the shard,
// unless it is a member: it holds a copy that no member reads.
func (s *controller) addPendingDelete(server *proto.DataServerIdentity) error {
	return s.metadataStore.PatchShardStatus(s.namespace, s.shard, func(current *proto.ShardMetadata) (bool, error) {
		if slices.ContainsFunc(current.Ensemble, dataserver.MatchDataServer(server)) {
			return false, nil
		}
		pending := len(current.PendingDeleteShardNodes)
		current.PendingDeleteShardNodes = dataserver.AddDataServer(current.PendingDeleteShardNodes, server)
		return len(current.PendingDeleteShardNodes) > pending, nil
	})
}

// growthBackingOff reports until when the next growth onto the data server
// waits, and whether it still does.
func (s *controller) growthBackingOff(to *proto.DataServerIdentity) (time.Time, bool) {
	backoff, found := s.growthBackoffs[to.GetNameOrDefault()]
	return backoff.until, found && time.Now().Before(backoff.until)
}

// backOffGrowth records a failed growth onto the data server: the next one
// waits growthBackoff, doubled at each failure in a row, up to
// growthBackoffCap.
func (s *controller) backOffGrowth(to *proto.DataServerIdentity) {
	name := to.GetNameOrDefault()
	backoff := s.growthBackoffs[name]
	backoff.failures++
	wait := min(s.growthBackoff, s.growthBackoffCap)
	for range backoff.failures - 1 {
		wait = min(wait*2, s.growthBackoffCap)
	}
	backoff.until = time.Now().Add(wait)
	s.growthBackoffs[name] = backoff
	s.logger.Info("The growth onto a data server backs off", slog.Any("to", to),
		slog.Int("failures", backoff.failures), slog.Duration("wait", wait))
}

// settleGrowthBackoff accounts for the growth onto pendingGrowthTo once it is
// settled: finished, it clears the failures of its new member, and undone, it
// backs it off.
func (s *controller) settleGrowthBackoff(shardMeta *proto.ShardMetadata) {
	to := s.pendingGrowthTo
	if to == nil {
		return
	}
	s.pendingGrowthTo = nil
	if slices.ContainsFunc(shardMeta.Ensemble, dataserver.MatchDataServer(to)) {
		delete(s.growthBackoffs, to.GetNameOrDefault())
		return
	}
	s.backOffGrowth(to)
}

// syncGrowthDeadline arms the deadline of a pending growth, unless it is armed
// already, and drops it when no growth is pending. It runs after every election,
// so that a coordinator that takes over a pending growth arms it too.
func (s *controller) syncGrowthDeadline() {
	shardMeta, err := s.loadShardMeta()
	if err != nil {
		s.logger.Warn("Failed to read the pending growth of the ensemble", slog.Any("error", err))
		return
	}
	if !isGrowth(shardMeta) {
		s.settleGrowthBackoff(shardMeta)
		s.stopGrowthDeadline()
		s.growthOverdue = false
		return
	}
	s.pendingGrowthTo = shardMeta.EnsembleChange.To
	if s.growthDeadline != nil || s.growthOverdue {
		return
	}
	to := shardMeta.EnsembleChange.To
	s.growthDeadline = time.AfterFunc(s.growthFinishTimeout, func() {
		channel.PushNoBlock(s.growthDeadlineOp, to)
	})
}

func (s *controller) stopGrowthDeadline() {
	if s.growthDeadline != nil {
		s.growthDeadline.Stop()
		s.growthDeadline = nil
	}
}

// onGrowthDeadline settles a growth that is still pending at its deadline with
// an election, which undoes it if its new member isn't among the highest
// heads. The elections that follow keep undoing it until one does, or the
// growth finishes.
func (s *controller) onGrowthDeadline(to *proto.DataServerIdentity) {
	s.growthDeadline = nil
	shardMeta, err := s.loadShardMeta()
	if err != nil {
		s.logger.Warn("Failed to read the pending growth of the ensemble at its deadline", slog.Any("error", err))
		// Not once the controller stopped: its shard was removed
		if s.ctx.Err() == nil {
			time.AfterFunc(time.Second, func() { channel.PushNoBlock(s.growthDeadlineOp, to) })
		}
		return
	}
	if !isGrowth(shardMeta) || !dataserver.MatchDataServer(to)(shardMeta.EnsembleChange.To) {
		return
	}
	s.logger.Warn("The growth of the ensemble missed its deadline, settling it", slog.Any("to", to))
	s.growthOverdue = true
	_, _ = s.electLeader(shardMeta, nil, "")
}

// onGrowthMemberFailure settles a pending growth whose new member failed with
// an election, which undoes it if a majority of the ensemble can't be fenced.
func (s *controller) onGrowthMemberFailure(shardMeta *proto.ShardMetadata, failed *proto.DataServerIdentity) bool {
	if !isGrowth(shardMeta) || !dataserver.MatchDataServer(failed)(shardMeta.EnsembleChange.To) {
		return false
	}
	s.logger.Info("The new member of a pending growth of the ensemble failed, settling the growth",
		slog.Any("to", failed))
	_, _ = s.electLeader(shardMeta, nil, "")
	return true
}

// loadShardMeta returns the status of the shard. A shard that was removed
// stops the controller, see stopRemoved.
func (s *controller) loadShardMeta() (*proto.ShardMetadata, error) {
	borrowedMeta, exists, err := s.metadataStore.GetShardStatus(s.namespace, s.shard)
	if err != nil {
		return nil, err
	}
	if !exists {
		s.stopRemoved("the growth of the ensemble")
		return nil, errors.Wrapf(errs.ErrNotFound, "shard %d", s.shard)
	}
	return borrowedMeta.UnsafeBorrow(), nil
}
