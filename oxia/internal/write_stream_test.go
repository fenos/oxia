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

package internal

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
)

type fakeRecv struct {
	response *proto.WriteResponse
	err      error
}

// fakeWriteStream is a controllable stand-in for the gRPC write stream:
// the test observes what was sent and injects responses or a failure.
type fakeWriteStream struct {
	grpc.ClientStream

	mu    sync.Mutex
	sent  []*proto.WriteRequest
	recvC chan fakeRecv
}

func newFakeWriteStream() *fakeWriteStream {
	return &fakeWriteStream{recvC: make(chan fakeRecv, 16)}
}

func (f *fakeWriteStream) Send(req *proto.WriteRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, req)
	return nil
}

func (f *fakeWriteStream) Recv() (*proto.WriteResponse, error) {
	r := <-f.recvC
	return r.response, r.err
}

func (f *fakeWriteStream) CloseSend() error { return nil }

func (f *fakeWriteStream) sentRequests() []*proto.WriteRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*proto.WriteRequest(nil), f.sent...)
}

func (f *fakeWriteStream) respond(resp *proto.WriteResponse) {
	f.recvC <- fakeRecv{response: resp}
}

func (f *fakeWriteStream) fail(err error) {
	f.recvC <- fakeRecv{err: err}
}

// fakeOpener hands pre-arranged streams to the wrapper's recovery, one per
// open call, and records the hints it was steered with.
type fakeOpener struct {
	streamC chan *fakeWriteStream

	mu    sync.Mutex
	hints []constant.ErrorMetadata
}

func newFakeOpener() *fakeOpener {
	return &fakeOpener{streamC: make(chan *fakeWriteStream, 16)}
}

func (o *fakeOpener) open(hint constant.ErrorMetadata) (proto.OxiaClient_WriteStreamClient, context.CancelFunc, error) {
	o.mu.Lock()
	o.hints = append(o.hints, hint)
	o.mu.Unlock()
	return <-o.streamC, func() {}, nil
}

func writeReq(key string) *proto.WriteRequest {
	shard := int64(1)
	return &proto.WriteRequest{
		Shard: &shard,
		Puts:  []*proto.PutRequest{{Key: key}},
	}
}

func waitFuture(t *testing.T, f concurrent.Future[*proto.WriteResponse]) (*proto.WriteResponse, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return f.Wait(ctx)
}

func TestWriteStreamPipelinesWithoutWaiting(t *testing.T) {
	opener := newFakeOpener()
	sw := newStreamWrapper(context.Background(), 1, opener.open, 10*time.Second)
	defer sw.closeWith(errors.New("test over"))

	stream := newFakeWriteStream()
	opener.streamC <- stream

	f1, err := sw.SendAsync(writeReq("a"))
	require.NoError(t, err)
	f2, err := sw.SendAsync(writeReq("b"))
	require.NoError(t, err)
	f3, err := sw.SendAsync(writeReq("c"))
	require.NoError(t, err)

	// All three are on the wire with no response delivered yet.
	assert.Eventually(t, func() bool { return len(stream.sentRequests()) == 3 },
		10*time.Second, time.Millisecond)

	// Responses complete the futures in FIFO order.
	for i, f := range []concurrent.Future[*proto.WriteResponse]{f1, f2, f3} {
		stream.respond(&proto.WriteResponse{Puts: []*proto.PutResponse{{}}})
		response, err := waitFuture(t, f)
		require.NoError(t, err, "future %d", i)
		require.NotNil(t, response)
	}
}

func TestWriteStreamReplaysInOrderOnFailure(t *testing.T) {
	opener := newFakeOpener()
	sw := newStreamWrapper(context.Background(), 1, opener.open, 10*time.Second)
	defer sw.closeWith(errors.New("test over"))

	stream1 := newFakeWriteStream()
	opener.streamC <- stream1

	f1, _ := sw.SendAsync(writeReq("a"))
	f2, _ := sw.SendAsync(writeReq("b"))
	f3, _ := sw.SendAsync(writeReq("c"))

	assert.Eventually(t, func() bool { return len(stream1.sentRequests()) == 3 },
		10*time.Second, time.Millisecond)

	// The head write is answered, then the stream breaks (retryable).
	stream1.respond(&proto.WriteResponse{})
	if _, err := waitFuture(t, f1); err != nil {
		t.Fatalf("head write should have completed: %v", err)
	}
	stream1.fail(io.EOF) // retryable: the whole suffix replays, nothing fails

	// Recovery replays exactly the unanswered suffix, in order.
	stream2 := newFakeWriteStream()
	opener.streamC <- stream2

	assert.Eventually(t, func() bool { return len(stream2.sentRequests()) == 2 },
		10*time.Second, time.Millisecond)
	sent := stream2.sentRequests()
	require.Equal(t, "b", sent[0].Puts[0].Key)
	require.Equal(t, "c", sent[1].Puts[0].Key)

	// Their futures never failed — the replay's responses complete them.
	stream2.respond(&proto.WriteResponse{})
	stream2.respond(&proto.WriteResponse{})
	_, err := waitFuture(t, f2)
	require.NoError(t, err)
	_, err = waitFuture(t, f3)
	require.NoError(t, err)
}

func TestWriteStreamNewSendsQueueBehindReplay(t *testing.T) {
	opener := newFakeOpener()
	sw := newStreamWrapper(context.Background(), 1, opener.open, 10*time.Second)
	defer sw.closeWith(errors.New("test over"))

	stream1 := newFakeWriteStream()
	opener.streamC <- stream1

	f1, _ := sw.SendAsync(writeReq("a"))
	f2, _ := sw.SendAsync(writeReq("b"))

	assert.Eventually(t, func() bool { return len(stream1.sentRequests()) == 2 },
		10*time.Second, time.Millisecond)

	// Break the stream; while recovery waits for a new stream, submit a
	// third write. It must transmit strictly behind the replayed ones.
	stream1.fail(io.EOF)

	f3, err := sw.SendAsync(writeReq("c"))
	require.NoError(t, err)

	stream2 := newFakeWriteStream()
	opener.streamC <- stream2

	assert.Eventually(t, func() bool { return len(stream2.sentRequests()) == 3 },
		10*time.Second, time.Millisecond)
	sent := stream2.sentRequests()
	require.Equal(t, "a", sent[0].Puts[0].Key)
	require.Equal(t, "b", sent[1].Puts[0].Key)
	require.Equal(t, "c", sent[2].Puts[0].Key)

	for _, f := range []concurrent.Future[*proto.WriteResponse]{f1, f2, f3} {
		stream2.respond(&proto.WriteResponse{})
		_, err := waitFuture(t, f)
		require.NoError(t, err)
	}
}

func TestWriteStreamNonRetryableFailsHeadOnly(t *testing.T) {
	opener := newFakeOpener()
	sw := newStreamWrapper(context.Background(), 1, opener.open, 10*time.Second)
	defer sw.closeWith(errors.New("test over"))

	stream1 := newFakeWriteStream()
	opener.streamC <- stream1

	f1, _ := sw.SendAsync(writeReq("a"))
	f2, _ := sw.SendAsync(writeReq("b"))

	assert.Eventually(t, func() bool { return len(stream1.sentRequests()) == 2 },
		10*time.Second, time.Millisecond)

	// A non-retryable failure answers the head write; the rest replay.
	permanent := errors.New("permanent condition")
	stream1.fail(permanent)

	_, err := waitFuture(t, f1)
	require.ErrorIs(t, err, permanent)

	stream2 := newFakeWriteStream()
	opener.streamC <- stream2

	assert.Eventually(t, func() bool { return len(stream2.sentRequests()) == 1 },
		10*time.Second, time.Millisecond)
	require.Equal(t, "b", stream2.sentRequests()[0].Puts[0].Key)

	stream2.respond(&proto.WriteResponse{})
	_, err = waitFuture(t, f2)
	require.NoError(t, err)
}

func TestWriteStreamHeadTimeoutFailsEverythingTogether(t *testing.T) {
	opener := newFakeOpener()
	sw := newStreamWrapper(context.Background(), 1, opener.open, 200*time.Millisecond)

	stream := newFakeWriteStream()
	opener.streamC <- stream

	f1, _ := sw.SendAsync(writeReq("a"))
	f2, _ := sw.SendAsync(writeReq("b"))

	// No responses ever arrive: the watchdog must fail both writes and
	// close the wrapper — never fail the head alone, and never leave an
	// abandoned write behind to be replayed.
	_, err := waitFuture(t, f1)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	_, err = waitFuture(t, f2)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	assert.Eventually(t, sw.isClosed, 10*time.Second, time.Millisecond)

	_, err = sw.SendAsync(writeReq("c"))
	require.ErrorIs(t, err, constant.ErrResourceUnavailable)
}

func TestWriteStreamCloseFailsPending(t *testing.T) {
	opener := newFakeOpener()
	sw := newStreamWrapper(context.Background(), 1, opener.open, 10*time.Second)

	stream := newFakeWriteStream()
	opener.streamC <- stream

	f1, _ := sw.SendAsync(writeReq("a"))

	closing := errors.New("client closing")
	sw.closeWith(closing)

	_, err := waitFuture(t, f1)
	require.ErrorIs(t, err, closing)
	require.True(t, sw.isClosed())
}
