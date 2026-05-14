// Copyright 2023-2025 The Oxia Authors
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

package client

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/oxia"
	"github.com/oxia-db/oxia/oxiad/dataserver"
)

// TestAsyncClient_List_CtxCancelDoesNotPanic exercises the bug fixed
// in async_client_impl.go: when a List() caller cancels ctx while a
// per-shard worker is still inside backoff retry, the worker's
// final `ch <- ListResult{Err: err}` raced against the closer's
// `wg.Wait(ctx); close(ch)` and crashed the process with
// `panic: send on closed channel`.
//
// The fix has the worker `select` between the send and ctx.Done, and
// the closer wait for workers unconditionally. This test fires many
// List() calls in parallel, cancels ctx mid-list, and asserts none
// of them panic. Without the fix the test fails intermittently with
// the panic; with the fix it's deterministic-clean.
func TestAsyncClient_List_CtxCancelDoesNotPanic(t *testing.T) {
	config := dataserver.NewTestConfig(t.TempDir())
	config.NumShards = 4 // multi-shard fan-out so the List closer goroutine fires
	standaloneServer, err := dataserver.NewStandalone(config)
	require.NoError(t, err)
	defer func() { assert.NoError(t, standaloneServer.Close()) }()

	client, err := oxia.NewAsyncClient(standaloneServer.ServiceAddr())
	require.NoError(t, err)
	defer func() { assert.NoError(t, client.Close()) }()

	// Seed enough keys per shard that listFromShard's gRPC stream has
	// a non-trivial Recv loop — gives the race window something to
	// hit even on a fast in-process server.
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("/key-%04d", i)
		<-client.Put(key, []byte(key))
	}

	// Many parallel List calls, each canceled almost immediately so
	// the worker's terminal send overlaps with the closer's close.
	const iterations = 500
	var wg sync.WaitGroup
	wg.Add(iterations)
	for i := 0; i < iterations; i++ {
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			ch := client.List(ctx, "/key-", "/key-\xff")
			// Read at most one item, then cancel and abandon the channel.
			select {
			case <-ch:
			case <-time.After(time.Microsecond):
			}
			cancel()
		}()
	}
	wg.Wait()

	// If we got here without `panic: send on closed channel`, the
	// race is closed.
}

// TestAsyncClient_RangeScan_CtxCancelDoesNotPanic is the dual of
// the List test for the rangeScanFromShard / doRangeScan paths.
func TestAsyncClient_RangeScan_CtxCancelDoesNotPanic(t *testing.T) {
	config := dataserver.NewTestConfig(t.TempDir())
	config.NumShards = 4
	standaloneServer, err := dataserver.NewStandalone(config)
	require.NoError(t, err)
	defer func() { assert.NoError(t, standaloneServer.Close()) }()

	client, err := oxia.NewAsyncClient(standaloneServer.ServiceAddr())
	require.NoError(t, err)
	defer func() { assert.NoError(t, client.Close()) }()

	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("/key-%04d", i)
		<-client.Put(key, []byte(key))
	}

	const iterations = 500
	var wg sync.WaitGroup
	wg.Add(iterations)
	for i := 0; i < iterations; i++ {
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			ch := client.RangeScan(ctx, "/key-", "/key-\xff")
			select {
			case <-ch:
			case <-time.After(time.Microsecond):
			}
			cancel()
		}()
	}
	wg.Wait()
}
