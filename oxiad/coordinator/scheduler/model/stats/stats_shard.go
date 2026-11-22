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

package stats

type ShardStats struct {
	Ensemble []string
	Leader   string
	Changing bool
	// Steady is whether the shard has a leader and no election in progress
	Steady bool
	// EnsembleCaughtUp is whether every member of the ensemble caught up with
	// the leader since its election
	EnsembleCaughtUp bool
	Splitting        bool
	Splittable       bool
	Overshoot        float64
	// PendingDelete are the data servers whose copy of the shard is still to
	// be deleted: none of them can be added to the ensemble until it is
	PendingDelete []string
	// GrowthHeld is whether the growth of the shard waits after failures
	GrowthHeld bool
}
