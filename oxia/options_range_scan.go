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

type rangeScanOptions struct {
	listOptions
	includeValue bool
	limit        uint32
}

// RangeScanOption represents an option for the [SyncClient.RangeScan] operation.
type RangeScanOption interface {
	applyRangeScan(opts *rangeScanOptions)
}

func newRangeScanOptions(opts []RangeScanOption) *rangeScanOptions {
	rangeScanOpts := &rangeScanOptions{includeValue: true}
	for _, opt := range opts {
		opt.applyRangeScan(rangeScanOpts)
	}
	return rangeScanOpts
}

type limit uint32

func (l limit) applyRangeScan(opts *rangeScanOptions) {
	opts.limit = uint32(l)
}

// Limit ends a RangeScan after n records: the first n in the order it returns
// them, by key, or by secondary key with UseIndex. The servers stop each shard's
// scan at n records; older servers send the whole range, and the client stops
// reading at n. Zero, the default, returns every record of the range.
func Limit(n uint32) RangeScanOption {
	return limit(n)
}
