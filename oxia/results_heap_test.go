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

package oxia

import (
	"testing"

	"github.com/stretchr/testify/assert"
	pb "google.golang.org/protobuf/proto"
)

func resultsChannel(results ...GetResult) chan GetResult {
	ch := make(chan GetResult, len(results))
	for _, r := range results {
		ch <- r
	}
	close(ch)
	return ch
}

func collectKeys(ch chan GetResult) []string {
	var keys []string
	for r := range ch {
		keys = append(keys, r.Key)
	}
	return keys
}

// A range scan over a secondary index spans every shard; the merge must
// follow the index's order — secondary key, then record key — not the
// record keys, which the index deliberately orders differently.
func TestAggregateRangeScanAcrossShardsOrdersByIndexKey(t *testing.T) {
	// Each shard yields its own results already in index order.
	// The empty secondary key is legal and sorts before every other.
	shard1 := resultsChannel(
		GetResult{Key: "c", SecondaryIndexKey: pb.String("idx/1")},
		GetResult{Key: "a", SecondaryIndexKey: pb.String("idx/3")},
	)
	shard2 := resultsChannel(
		GetResult{Key: "e", SecondaryIndexKey: pb.String("")},
		GetResult{Key: "b", SecondaryIndexKey: pb.String("idx/2")},
		GetResult{Key: "d", SecondaryIndexKey: pb.String("idx/3")},
	)

	out := make(chan GetResult)
	go aggregateAndSortRangeScanAcrossShards([]chan GetResult{shard1, shard2}, out)

	assert.Equal(t, []string{"e", "c", "b", "a", "d"}, collectKeys(out),
		"secondary key ascending, the empty key first, ties by record key")
}

// Without an index, record keys order the merge as before.
func TestAggregateRangeScanAcrossShardsOrdersByKey(t *testing.T) {
	shard1 := resultsChannel(GetResult{Key: "a"}, GetResult{Key: "d"})
	shard2 := resultsChannel(GetResult{Key: "b"}, GetResult{Key: "c"})

	out := make(chan GetResult)
	go aggregateAndSortRangeScanAcrossShards([]chan GetResult{shard1, shard2}, out)

	assert.Equal(t, []string{"a", "b", "c", "d"}, collectKeys(out))
}
