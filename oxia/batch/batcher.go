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

func (b *batcherImpl) Run() { //nolint:revive
	var open Batch    // accumulating operations
	var ready []Batch // finished, parked awaiting window slots; oldest first
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
		open = b.batchFactory()
		if b.linger > 0 {
			timer = time.NewTimer(b.linger)
			timeout = timer.C
		}
	}

	// send transmits on an already-acquired slot. It never blocks: joinC's
	// capacity equals the window, and the completion goroutine takes a
	// join before freeing its slot.
	send := func(sender Sender) {
		b.joinC <- sender.Send()
	}

	// place hands a finished batch onward. Pipelined batches take a free
	// slot immediately, or park in a FIFO — bounded by the window depth —
	// while the window is exhausted, letting the next batch keep
	// accumulating: a saturated server gets fewer, fatter batches instead
	// of a fixed-size dribble. With the parking full too, place blocks
	// until a slot frees — that block is the batcher's backpressure.
	// Returns false on shutdown; the batch has been failed.
	place := func(batch Batch) bool {
		if b.linger > 0 {
			timer.Stop()
		}
		if b.pipelined() {
			if sender, ok := batch.(Sender); ok {
				// The direct path is only for an empty parking queue: with
				// older batches parked, a freed slot must never let this
				// newer batch overtake them — dispatch order is the
				// ordering the server observes.
				if len(ready) == 0 {
					select {
					case b.slots <- struct{}{}:
						send(sender)
						return true
					default:
					}
				}
				if len(ready) < b.maxBatchesInFlight {
					ready = append(ready, batch)
					return true
				}
				select {
				case b.slots <- struct{}{}:
					send(ready[0].(Sender))
					ready = append(ready[1:], batch)
					return true
				case <-b.closeC:
					batch.Fail(ErrShuttingDown)
					return false
				}
			}
		}
		batch.Complete()
		return true
	}

	for {
		// A parked batch dispatches the moment a slot frees; the case is
		// disabled (nil channel) while nothing is parked.
		var slotC chan<- struct{}
		if len(ready) > 0 {
			slotC = b.slots
		}

		select {
		case slotC <- struct{}{}:
			send(ready[0].(Sender))
			ready = ready[1:]

		case call := <-b.callC:
			if open == nil {
				newBatch()
			}
			if !open.CanAdd(call) {
				full := open
				open = nil
				if !place(full) {
					continue // shutting down; closeC case runs next
				}
				newBatch()
			}
			open.Add(call)
			if open.Size() == b.maxRequestsPerBatch || b.linger == 0 {
				full := open
				open = nil
				place(full)
			}

		case <-timeout:
			if open != nil {
				lingered := open
				open = nil
				place(lingered)
			}
		case <-b.closeC:
			if open != nil {
				timer.Stop()
				open.Fail(ErrShuttingDown)
				open = nil
			}
			for _, parked := range ready {
				parked.Fail(ErrShuttingDown)
			}
			ready = nil
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
