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
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia/batch"
	internalbatch "github.com/oxia-db/oxia/oxia/internal/batch"
	"github.com/oxia-db/oxia/oxia/internal/model"
)

// The first shard error terminates the result channel; the responses of the
// remaining shards — errors included — must be discarded, not panic with a
// send on the closed channel (the error path used to fall through and leave
// the counter sentinel negative, defeating the response-already-sent guard).
func TestMultiShardGetCallback_ErrorsAfterFirstAreDiscarded(t *testing.T) {
	ch := make(chan GetResult, 1)
	callback := multiShardGetCallback("key-a", proto.KeyComparisonType_FLOOR, 3, ch)

	callback(nil, errors.New("shard-0 failed"))
	result := <-ch
	require.Error(t, result.Err)

	assert.NotPanics(t, func() {
		callback(nil, errors.New("shard-1 failed"))
		callback(&proto.GetResponse{Status: proto.Status_KEY_NOT_FOUND}, nil)
	})

	// The channel was closed exactly once, after the single error result
	_, open := <-ch
	assert.False(t, open)
}

func TestMultiShardGetCallback_AllShardsRespond(t *testing.T) {
	ch := make(chan GetResult, 1)
	callback := multiShardGetCallback("key-a", proto.KeyComparisonType_FLOOR, 3, ch)

	for i := 0; i < 3; i++ {
		callback(&proto.GetResponse{Status: proto.Status_KEY_NOT_FOUND}, nil)
	}

	result := <-ch
	assert.ErrorIs(t, result.Err, ErrKeyNotFound)
	_, open := <-ch
	assert.False(t, open)
}

// capturingBatcher hands added calls to the test instead of batching them.
type capturingBatcher struct {
	calls chan any
}

func (b *capturingBatcher) Add(request any) { b.calls <- request }
func (b *capturingBatcher) Run()            {}
func (b *capturingBatcher) Close() error    { return nil }

// staticShardManager routes every key to shard 0.
type staticShardManager struct{}

func (staticShardManager) Close() error              { return nil }
func (staticShardManager) Get(string) int64          { return 0 }
func (staticShardManager) GetAll() []int64           { return []int64{0} }
func (staticShardManager) Leader(int64) string       { return "" }
func (staticShardManager) Exists(shardId int64) bool { return shardId == 0 }

// The sync wrapper abandons the Get result channel when the caller's
// context expires. The completion callback — which the batcher's Run
// goroutine executes inline — must not block on that abandoned channel:
// if it does, the batcher deadlocks and every subsequent read on the
// shard wedges inside Add, past any deadline. Regression test for the
// read path; the write path already buffers its result channels.
func TestGetCompletionDoesNotBlockWhenCallerAbandons(t *testing.T) {
	captured := &capturingBatcher{calls: make(chan any, 1)}
	c := &clientImpl{
		shardManager: staticShardManager{},
		readBatchManager: internalbatch.NewManager(context.Background(),
			func(context.Context, *int64) batch.Batcher { return captured }),
	}

	ch := c.Get("key")
	call, ok := (<-captured.calls).(model.GetCall)
	assert.True(t, ok)

	// Nobody is receiving from ch — the abandoned-caller shape.
	completed := make(chan struct{})
	go func() {
		call.Callback(&proto.GetResponse{Status: proto.Status_KEY_NOT_FOUND}, nil)
		close(completed)
	}()

	select {
	case <-completed:
		// The completion path survives an abandoned caller.
	case <-time.After(3 * time.Second):
		t.Fatal("completion callback blocked on the abandoned result channel; " +
			"this deadlocks the batcher's Run loop and wedges all subsequent reads on the shard")
	}

	// A caller that did wait still receives the result.
	result := <-ch
	assert.ErrorIs(t, result.Err, ErrKeyNotFound)
}
