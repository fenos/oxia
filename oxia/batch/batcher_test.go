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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type testBatch struct {
	count  int
	calls  []any
	result chan error
}

func newTestBatch() *testBatch {
	return &testBatch{
		calls:  make([]any, 0),
		result: make(chan error, 1),
	}
}

func (b *testBatch) CanAdd(call any) bool {
	return true
}

func (b *testBatch) Add(call any) {
	b.count++
}

func (b *testBatch) Size() int {
	return b.count
}

func (b *testBatch) Complete() {
	close(b.result)
}

func (b *testBatch) Fail(err error) {
	b.result <- err
	// closeC(b.result)
}

func TestBatcher(t *testing.T) {
	for _, item := range []struct {
		name             string
		linger           time.Duration
		maxSize          int
		closeImmediately bool
		expectedErr      error
	}{
		{"complete on maxRequestsPerBatch", 1 * time.Second, 1, false, nil},
		{"complete on linger", 1 * time.Millisecond, 2, false, nil},
		{"fail on close", 1 * time.Second, 2, true, ErrShuttingDown},
	} {
		t.Run(item.name, func(t *testing.T) {
			testBatch := newTestBatch()

			batchFactory := func() Batch {
				return testBatch
			}

			factory := &BatcherFactory{
				Linger:              item.linger,
				MaxRequestsPerBatch: item.maxSize,
			}
			batcher := factory.NewBatcher(context.Background(), 1, "test-write", batchFactory, 1)
			batcher.Add(1)

			if item.closeImmediately {
				err := batcher.Close()
				assert.NoError(t, err)
			}

			assert.ErrorIs(t, <-testBatch.result, item.expectedErr)

			if !item.closeImmediately {
				err := batcher.Close()
				assert.NoError(t, err)
			}
		})
	}
}

// sendableBatch implements Sender: its join blocks until the test releases
// it, letting the test hold the window open and observe dispatch order.
type sendableBatch struct {
	id      int
	count   int
	sentC   chan *sendableBatch
	joinedC chan *sendableBatch
	release chan struct{}
	failC   chan error
}

func (b *sendableBatch) CanAdd(any) bool { return true }
func (b *sendableBatch) Add(any)         { b.count++ }
func (b *sendableBatch) Size() int       { return b.count }
func (b *sendableBatch) Complete()       { b.Send()() }
func (b *sendableBatch) Fail(err error)  { b.failC <- err }

func (b *sendableBatch) Send() func() {
	b.sentC <- b
	return func() {
		<-b.release
		b.joinedC <- b
	}
}

func TestBatcherPipelinesWithinWindow(t *testing.T) {
	sentC := make(chan *sendableBatch, 16)
	joinedC := make(chan *sendableBatch, 16)
	failC := make(chan error, 16)

	nextID := 0
	batchFactory := func() Batch {
		nextID++
		return &sendableBatch{
			id:      nextID,
			sentC:   sentC,
			joinedC: joinedC,
			release: make(chan struct{}),
			failC:   failC,
		}
	}

	factory := &BatcherFactory{
		Linger:              1 * time.Hour, // dispatch on size only
		MaxRequestsPerBatch: 1,
	}
	batcher := factory.NewBatcher(context.Background(), 1, "test-write", batchFactory, 3)
	defer batcher.Close()

	for i := 0; i < 5; i++ {
		batcher.Add(i)
	}

	// Three batches transmit without any outcome delivered — the window —
	// and they transmit in submission order.
	var inFlight []*sendableBatch
	for i := 0; i < 3; i++ {
		select {
		case b := <-sentC:
			inFlight = append(inFlight, b)
		case <-time.After(10 * time.Second):
			t.Fatalf("batch %d never transmitted", i+1)
		}
	}
	assert.Equal(t, []int{1, 2, 3}, []int{inFlight[0].id, inFlight[1].id, inFlight[2].id})

	// The fourth waits for a slot.
	select {
	case b := <-sentC:
		t.Fatalf("batch %d transmitted beyond the window", b.id)
	case <-time.After(50 * time.Millisecond):
	}

	// Releasing the head frees one slot: exactly one more transmits, and
	// the released batch joins first — completion follows dispatch order.
	close(inFlight[0].release)
	assert.Equal(t, 1, (<-joinedC).id)
	b4 := <-sentC
	assert.Equal(t, 4, b4.id)

	// Drain the rest: joins complete in dispatch order throughout.
	close(inFlight[1].release)
	close(inFlight[2].release)
	assert.Equal(t, 2, (<-joinedC).id)
	assert.Equal(t, 3, (<-joinedC).id)
	b5 := <-sentC
	assert.Equal(t, 5, b5.id)
	close(b4.release)
	close(b5.release)
	assert.Equal(t, 4, (<-joinedC).id)
	assert.Equal(t, 5, (<-joinedC).id)
}

func TestBatcherCloseFinishesInFlightJoins(t *testing.T) {
	sentC := make(chan *sendableBatch, 16)
	joinedC := make(chan *sendableBatch, 16)
	failC := make(chan error, 16)

	nextID := 0
	batchFactory := func() Batch {
		nextID++
		return &sendableBatch{
			id:      nextID,
			sentC:   sentC,
			joinedC: joinedC,
			release: make(chan struct{}),
			failC:   failC,
		}
	}

	factory := &BatcherFactory{
		Linger:              1 * time.Hour,
		MaxRequestsPerBatch: 1,
	}
	batcher := factory.NewBatcher(context.Background(), 1, "test-write", batchFactory, 2)

	batcher.Add(1)
	batcher.Add(2)

	b1 := <-sentC
	b2 := <-sentC

	// Close with both joins outstanding: they must still run to
	// completion — their callers get real outcomes, not silence.
	assert.NoError(t, batcher.Close())
	close(b1.release)
	close(b2.release)
	assert.Equal(t, b1.id, (<-joinedC).id)
	assert.Equal(t, b2.id, (<-joinedC).id)
}
