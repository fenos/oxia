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

import (
	"errors"
	"io"
	"sync/atomic"
	"time"
)

var ErrShuttingDown = errors.New("shutting down")

type Batcher interface {
	io.Closer
	Add(request any)
	Run()
}

type batcherImpl struct {
	batchFactory        func() Batch
	callC               chan any
	closeC              chan bool
	closed              atomic.Bool
	linger              time.Duration
	maxRequestsPerBatch int

	// maxBatchesInFlight bounds how many dispatched batches may be
	// awaiting their outcome at once. Above one, batches implementing
	// Sender are pipelined: slots is the window occupancy, joinC carries
	// their joins to the completion goroutine in dispatch order, so
	// callbacks fire in the order the batches were transmitted.
	maxBatchesInFlight int
	slots              chan struct{}
	joinC              chan func()
	joinsDone          chan struct{}
}

func (b *batcherImpl) Close() error {
	b.closed.Store(true)
	close(b.closeC)
	return nil
}

func (b *batcherImpl) Add(call any) {
	if b.closed.Load() {
		b.failCall(call, ErrShuttingDown)
	} else {
		b.callC <- call
	}
}

func (b *batcherImpl) failCall(call any, err error) {
	batch := b.batchFactory()
	batch.Add(call)
	batch.Fail(err)
}

func (b *batcherImpl) pipelined() bool {
	return b.maxBatchesInFlight > 1
}

// dispatch hands a full (or lingered) batch onward. A pipelining batcher
// transmits it and queues the join without waiting for the outcome,
// blocking only while the in-flight window is exhausted — that block is
// the backpressure. Otherwise the batch completes inline. Returns false
// when the batcher shut down instead of dispatching; the batch is failed.
func (b *batcherImpl) dispatch(batch Batch) bool {
	if b.pipelined() {
		if sender, ok := batch.(Sender); ok {
			select {
			case b.slots <- struct{}{}:
			case <-b.closeC:
				batch.Fail(ErrShuttingDown)
				return false
			}
			// Never blocks: joinC's capacity equals the window, and the
			// completion goroutine takes a join before freeing its slot.
			b.joinC <- sender.Send()
			return true
		}
	}
	batch.Complete()
	return true
}

func (b *batcherImpl) Run() { //nolint:revive
	var batch Batch
	var timer *time.Timer
	var timeout <-chan time.Time

	if b.pipelined() {
		b.slots = make(chan struct{}, b.maxBatchesInFlight)
		b.joinC = make(chan func(), b.maxBatchesInFlight)
		b.joinsDone = make(chan struct{})

		go func() {
			defer close(b.joinsDone)
			for join := range b.joinC {
				join()
				<-b.slots
			}
		}()
	}

	newBatch := func() {
		batch = b.batchFactory()
		if b.linger > 0 {
			timer = time.NewTimer(b.linger)
			timeout = timer.C
		}
	}
	dispatchBatch := func() {
		if b.linger > 0 {
			timer.Stop()
		}
		toSend := batch
		batch = nil
		b.dispatch(toSend)
	}

	for {
		select {
		case call := <-b.callC:
			if batch == nil {
				newBatch()
			}
			canAdd := batch.CanAdd(call)
			if !canAdd {
				dispatchBatch()
				newBatch()
			}
			batch.Add(call)
			if batch.Size() == b.maxRequestsPerBatch || b.linger == 0 {
				dispatchBatch()
			}

		case <-timeout:
			if batch != nil {
				dispatchBatch()
			}
		case <-b.closeC:
			if batch != nil {
				timer.Stop()
				batch.Fail(ErrShuttingDown)
				batch = nil
			}
			for {
				select {
				case call := <-b.callC:
					b.failCall(call, ErrShuttingDown)
				default:
					// In-flight batches still finish: their joins run to
					// completion before the batcher's goroutines exit.
					if b.pipelined() {
						close(b.joinC)
						<-b.joinsDone
					}
					return
				}
			}
		}
	}
}
