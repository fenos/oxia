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

package assignment

import (
	"context"
	"errors"
	"log/slog"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// contextStream is a stream whose Recv fails as soon as the context ends,
// as a gRPC stream does when its server closes.
type contextStream struct {
	ctx context.Context
}

func (s *contextStream) Recv() (*int, error) {
	<-s.ctx.Done()
	return nil, errors.New("stream closed")
}

// TestStreamReaderGoroutineEndsAfterRunReturnedOnContext: when the server
// context ends, Run returns at once and the reader goroutine fails its Recv
// and reports the error. Nobody is listening any more: the report must not
// park the goroutine, and it must not race Run's own read of the channel.
func TestStreamReaderGoroutineEndsAfterRunReturnedOnContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	stream := &contextStream{ctx: ctx}

	before := runtime.NumGoroutine()

	reader := ReadStream(ctx, stream, func(*int) error { return nil }, nil, slog.Default())
	cancel()
	// The context ends Run and fails the stream at once; the reader's
	// report races Run's return and reaches no one.
	require.NoError(t, reader.Run(), "Run returns on the context without an error")

	// Polled inline: a helper that runs the condition on its own goroutine
	// would keep the count above the baseline by itself.
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > before {
		if time.Now().After(deadline) {
			t.Fatalf("the reader goroutine did not end: %d goroutines, %d before", runtime.NumGoroutine(), before)
		}

		time.Sleep(10 * time.Millisecond)
	}
}
