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

package scheduler

import (
	"log/slog"
	"time"

	"github.com/oxia-db/oxia/oxiad/coordinator/action"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model/stats"
)

// This file owns the holds on the growth of shards; scheduler_default.go runs
// the rounds that keep them.
//
// The planner grows a shard short of its replication factor before anything
// else, and a round that plans an op samples no split. A shard whose growth
// keeps failing, e.g. one too hot for a new member to catch up with, would so
// starve the splits that could cool it, whichever data server each growth
// tries. A shard whose growth failed is held: the planner doesn't grow it for
// growthHold, doubled at each failure in a row up to growthHoldCap, and the
// rounds meanwhile plan the other ops, the splits included.

const (
	// growthHold is how long the growth of a shard waits after one failed.
	growthHold = time.Minute
	// growthHoldCap bounds the wait after failures in a row.
	growthHoldCap = 30 * time.Minute
)

// shardHold is the failures in a row of the growths of a shard, and until
// when the next one waits.
type shardHold struct {
	failures int
	until    time.Time
}

// growthHolds are the holds on the growth of shards, by shard. The rounds of
// the scheduler own them.
type growthHolds map[int64]shardHold

// account records the outcome of the growths among the results of a round:
// a growth that failed holds its shard, and one that succeeded releases it.
func (h growthHolds) account(results []model.Result, now time.Time, logger *slog.Logger) {
	for _, result := range results {
		change, ok := result.Action.(*action.ChangeEnsembleAction)
		if !ok || change.From != nil {
			continue
		}
		if result.Err == nil {
			delete(h, change.Shard)
			continue
		}
		hold := h[change.Shard]
		hold.failures++
		wait := growthHold
		for range hold.failures - 1 {
			wait = min(wait*2, growthHoldCap)
		}
		hold.until = now.Add(wait)
		h[change.Shard] = hold
		logger.Info("The growth of a shard is held", slog.Int64("shard", change.Shard),
			slog.Int("failures", hold.failures), slog.Duration("wait", wait))
	}
}

// mark marks the shards of s whose growth is held at now, and forgets the
// holds of the shards that s no longer has, e.g. split or deleted.
func (h growthHolds) mark(s *stats.ClusterStats, now time.Time) {
	held := make(map[int64]*stats.ShardStats, len(h))
	for _, ns := range s.Namespaces {
		for id, shard := range ns.Shards {
			if _, found := h[id]; found {
				held[id] = shard
			}
		}
	}
	for id, hold := range h {
		shard, found := held[id]
		if !found {
			delete(h, id)
			continue
		}
		shard.GrowthHeld = now.Before(hold.until)
	}
}
