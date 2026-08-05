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

package batch

type Batch interface {
	CanAdd(any) bool
	Add(any)
	Size() int
	Complete()
	Fail(error)
}

// Sender is implemented by batches whose transmission can be decoupled
// from their completion: Send transmits the batch without waiting for the
// response and returns a join that finishes it — waiting for the outcome
// and running the callbacks — exactly once when invoked.
//
// A batcher configured with a batches-in-flight window above one pipelines
// such batches: it keeps forming and transmitting new batches while up to
// that many joins are outstanding, instead of blocking on Complete after
// every batch. Batches that do not implement Sender always complete
// inline, one at a time.
type Sender interface {
	Send() func()
}
