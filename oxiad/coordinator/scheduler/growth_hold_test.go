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
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/oxiad/coordinator/action"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model/stats"
)

// A failed growth holds its shard, for growthHold doubled at each failure in a
// row up to growthHoldCap; a growth that succeeds releases it, and the other
// actions don't count.
func TestGrowthHoldsAccountForTheGrowths(t *testing.T) {
	h := make(growthHolds)
	now := time.Now()
	grow := func(shard int64, err error) model.Result {
		return model.Result{Action: action.NewChangeEnsembleAction(shard, nil, identities("n1")[0]), Err: err}
	}
	failed := errors.New("the new member didn't catch up in time")

	h.account([]model.Result{
		grow(1, failed),
		{Action: action.NewChangeEnsembleAction(2, identities("n0")[0], identities("n1")[0]), Err: failed},
		{Action: action.NewSplitAction("a", 3), Err: failed},
	}, now, slog.Default())
	assert.Equal(t, growthHolds{1: {failures: 1, until: now.Add(growthHold)}}, h)

	for range 10 {
		h.account([]model.Result{grow(1, failed)}, now, slog.Default())
	}
	assert.Equal(t, now.Add(growthHoldCap), h[1].until, "doubled, up to the bound")
	assert.Equal(t, 11, h[1].failures)

	h.account([]model.Result{grow(1, nil)}, now, slog.Default())
	assert.Empty(t, h)
}

// The shards whose growth is held are marked for the planner while the hold
// lasts; the hold of a shard that is gone is forgotten.
func TestGrowthHoldsMarkTheHeldShards(t *testing.T) {
	now := time.Now()
	h := growthHolds{
		1: {failures: 1, until: now.Add(time.Minute)},
		2: {failures: 1, until: now.Add(-time.Second)},
		3: {failures: 1, until: now.Add(time.Minute)},
	}
	s := &stats.ClusterStats{Namespaces: map[string]*stats.NamespaceStats{
		"a": {Shards: map[int64]*stats.ShardStats{1: {}, 2: {GrowthHeld: true}, 4: {}}},
	}}

	h.mark(s, now)

	shards := s.Namespaces["a"].Shards
	assert.True(t, shards[1].GrowthHeld)
	assert.False(t, shards[2].GrowthHeld, "its hold expired")
	assert.False(t, shards[4].GrowthHeld)
	assert.NotContains(t, h, int64(3), "the shard is gone")
	assert.Contains(t, h, int64(2), "the failures in a row are kept")
}
