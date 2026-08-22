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
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/common/proto"
	commontime "github.com/oxia-db/oxia/common/time"
)

// writeStreamOpener opens a write stream to the current leader of the
// wrapper's shard, optionally steered by a leader hint from a previous
// failure. The returned cancel tears the stream down.
type writeStreamOpener func(hint constant.ErrorMetadata) (proto.OxiaClient_WriteStreamClient, context.CancelFunc, error)

// inflightWrite is one request travelling through the stream: the request
// is retained so it can be replayed, in order, on a recovered stream.
// sentAt is the original submission time — replay does not reset it, so
// the timeout measures the age the caller observes.
type inflightWrite struct {
	request *proto.WriteRequest
	future  concurrent.Future[*proto.WriteResponse]
	sentAt  time.Time
}

// streamWrapper pipelines write batches over one per-shard gRPC stream
// while preserving submission order across stream failures.
//
// All in-flight writes live in a single FIFO queue. Sending appends and
// writes to the live stream; responses complete from the head (the stream
// is FIFO). When the stream breaks, the queue is NOT failed: the stream is
// discarded and a recovery goroutine opens a new one (steered by the
// error's leader hint) and replays the whole queue in original order. New
// sends during recovery append to the same queue under the same lock, so a
// newer write can never overtake a replayed one — ordering is total, which
// is what keeps compare-and-set sequences issued in order from failing
// spuriously after a leader change.
//
// A non-retryable error fails only the head write — the one the error
// answers — and the rest keep replaying. A head write older than
// requestTimeout closes the whole wrapper, failing every in-flight write
// together: entries are never removed from the middle of the queue, since
// that would desynchronize the FIFO response matching, and a timed-out
// write must never be replayed after its caller saw the error. The
// provider replaces a closed wrapper on the next use.
type streamWrapper struct {
	sync.Mutex

	shard          int64
	ctx            context.Context
	open           writeStreamOpener
	requestTimeout time.Duration

	stream       proto.OxiaClient_WriteStreamClient // nil while broken/recovering
	streamCancel context.CancelFunc
	everOpened   bool // distinguishes the first open from a true recovery
	inflight     []*inflightWrite
	hint         constant.ErrorMetadata

	closed   bool
	closedC  chan struct{}
	recoverC chan struct{}
}

func newStreamWrapper(ctx context.Context, shard int64, open writeStreamOpener, requestTimeout time.Duration) *streamWrapper {
	sw := &streamWrapper{
		shard:          shard,
		ctx:            ctx,
		open:           open,
		requestTimeout: requestTimeout,
		closedC:        make(chan struct{}),
		recoverC:       make(chan struct{}, 1),
	}

	go process.DoWithLabels(ctx, map[string]string{
		"oxia":  "write-stream-recovery",
		"shard": fmt.Sprintf("%d", shard),
	}, sw.recoveryLoop)

	go process.DoWithLabels(ctx, map[string]string{
		"oxia":  "write-stream-timeout",
		"shard": fmt.Sprintf("%d", shard),
	}, sw.watchdogLoop)

	return sw
}

// SendAsync queues the request and transmits it without waiting for the
// response. The returned future completes when the response arrives — on
// this stream or, after a failure, on a recovered stream that replayed the
// request. It errs only when the wrapper is closed; a broken stream is
// recovered internally, never surfaced here.
func (sw *streamWrapper) SendAsync(request *proto.WriteRequest) (concurrent.Future[*proto.WriteResponse], error) {
	sw.Lock()

	if sw.closed {
		sw.Unlock()
		return nil, fmt.Errorf("oxia: write stream wrapper closed: %w", constant.ErrResourceUnavailable)
	}

	entry := &inflightWrite{
		request: request,
		future:  concurrent.NewFuture[*proto.WriteResponse](),
		sentAt:  time.Now(),
	}
	sw.inflight = append(sw.inflight, entry)

	var cancel context.CancelFunc
	needRecovery := sw.stream == nil
	if sw.stream != nil {
		if err := sw.stream.Send(request); err != nil {
			// The entry stays queued: recovery will replay it.
			sw.stream = nil
			cancel, sw.streamCancel = sw.streamCancel, nil
			needRecovery = true
		}
	}
	sw.Unlock()

	if cancel != nil {
		cancel()
	}
	if needRecovery {
		sw.nudgeRecovery()
	}
	return entry.future, nil
}

// Send is SendAsync plus waiting for the response.
func (sw *streamWrapper) Send(ctx context.Context, request *proto.WriteRequest) (*proto.WriteResponse, error) {
	f, err := sw.SendAsync(request)
	if err != nil {
		return nil, err
	}
	return f.Wait(ctx)
}

func (sw *streamWrapper) isClosed() bool {
	sw.Lock()
	defer sw.Unlock()
	return sw.closed
}

func (sw *streamWrapper) nudgeRecovery() {
	select {
	case sw.recoverC <- struct{}{}:
	default:
	}
}

// readLoop consumes one stream's responses, completing in-flight writes
// from the head of the queue. It exits when the stream errors, handing the
// failure to streamBroken. Responses from a stream the wrapper has already
// discarded are ignored: their writes are replayed on the new stream and
// completed by its responses.
func (sw *streamWrapper) readLoop(stream proto.OxiaClient_WriteStreamClient) {
	for {
		response, err := stream.Recv()
		if err != nil {
			sw.streamBroken(stream, err)
			return
		}

		sw.Lock()
		if sw.stream != stream {
			sw.Unlock()
			continue
		}

		var head *inflightWrite
		if len(sw.inflight) > 0 {
			head = sw.inflight[0]
			sw.inflight = sw.inflight[1:]
		}
		sw.Unlock()

		if head == nil {
			slog.Warn("Received write response with no inflight write",
				slog.Int64("shard", sw.shard))
			continue
		}
		head.future.Complete(response)
	}
}

// streamBroken discards a failed stream and kicks recovery. The in-flight
// queue survives — recovery replays it — except that a non-retryable error
// fails the head write, so a permanent condition drains the queue one
// answered write per attempt instead of replaying forever.
func (sw *streamWrapper) streamBroken(stream proto.OxiaClient_WriteStreamClient, err error) {
	sw.Lock()
	if sw.closed || sw.stream != stream {
		sw.Unlock()
		return
	}
	sw.stream = nil
	cancel := sw.streamCancel
	sw.streamCancel = nil

	headFail, pending, headErr := sw.noteFailureLocked(err)
	sw.Unlock()

	if cancel != nil {
		cancel()
	}
	slog.Warn("Write stream broken; recovering",
		slog.Int64("shard", sw.shard),
		slog.Int("pendingWrites", pending),
		slog.Any("error", err))

	if headFail != nil {
		headFail.future.Fail(headErr)
	}
	if pending > 0 {
		sw.nudgeRecovery()
	}
}

// noteFailureLocked records a failure's leader hint and, when the error is
// non-retryable, detaches the head write for the caller to fail with the
// translated error. Callers hold the lock; the future is failed outside it.
func (sw *streamWrapper) noteFailureLocked(err error) (headFail *inflightWrite, pending int, headErr error) {
	translated, md := constant.FromGrpcError(err)
	if len(md) > 0 {
		sw.hint = md
	}
	if !constant.IsRetryable(translated) && len(sw.inflight) > 0 {
		headFail = sw.inflight[0]
		sw.inflight = sw.inflight[1:]
	}
	return headFail, len(sw.inflight), translated
}

// recoveryLoop owns stream (re)creation: on a nudge it opens a new stream —
// steered by the last failure's leader hint — replays every in-flight
// write in order, and installs the stream. Failed attempts back off; a
// non-retryable failure fails the head write per attempt. It exits when
// the wrapper closes.
func (sw *streamWrapper) recoveryLoop() {
	for {
		select {
		case <-sw.recoverC:
		case <-sw.closedC:
			return
		case <-sw.ctx.Done():
			return
		}

		bo := commontime.NewBackOff(sw.ctx)
		for {
			sw.Lock()
			if sw.closed {
				sw.Unlock()
				return
			}
			if sw.stream != nil || len(sw.inflight) == 0 {
				sw.Unlock()
				break
			}
			hint := sw.hint
			sw.Unlock()

			err := sw.reconnect(hint)
			if err == nil {
				break
			}

			sw.Lock()
			headFail, pending, headErr := sw.noteFailureLocked(err)
			sw.Unlock()
			if headFail != nil {
				headFail.future.Fail(headErr)
			}

			slog.Warn("Write stream recovery attempt failed",
				slog.Int64("shard", sw.shard),
				slog.Int("pendingWrites", pending),
				slog.Any("error", err))

			delay := bo.NextBackOff()
			if delay == backoff.Stop {
				return
			}
			select {
			case <-time.After(delay):
			case <-sw.closedC:
				return
			case <-sw.ctx.Done():
				return
			}
		}
	}
}

// reconnect opens a stream and, under the lock, replays the in-flight
// queue in order before installing it. New sends block on the same lock
// meanwhile, so they queue — and transmit — strictly behind the replay.
func (sw *streamWrapper) reconnect(hint constant.ErrorMetadata) error {
	stream, cancel, err := sw.open(hint)
	if err != nil {
		return err
	}

	sw.Lock()
	if sw.closed {
		sw.Unlock()
		cancel()
		return nil
	}
	for _, entry := range sw.inflight {
		if err := stream.Send(entry.request); err != nil {
			sw.Unlock()
			cancel()
			return err
		}
	}
	sw.stream = stream
	sw.streamCancel = cancel
	pending := len(sw.inflight)
	recovered := sw.everOpened
	sw.everOpened = true
	sw.Unlock()

	go process.DoWithLabels(sw.ctx, map[string]string{
		"oxia":  "write-stream-read",
		"shard": fmt.Sprintf("%d", sw.shard),
	}, func() { sw.readLoop(stream) })

	if recovered {
		slog.Info("Write stream recovered; replayed inflight writes",
			slog.Int64("shard", sw.shard),
			slog.Int("pendingWrites", pending))
	} else {
		slog.Debug("Write stream opened",
			slog.Int64("shard", sw.shard),
			slog.Int("pendingWrites", pending))
	}
	return nil
}

// watchdogLoop bounds the age of the head in-flight write. When it exceeds
// requestTimeout the whole wrapper closes and every in-flight write fails
// together: failing only the head would desynchronize the FIFO response
// matching, and keeping the entry would replay a write whose caller
// already saw an error.
func (sw *streamWrapper) watchdogLoop() {
	interval := sw.requestTimeout / 10
	if interval < 10*time.Millisecond {
		interval = 10 * time.Millisecond
	}
	if interval > time.Second {
		interval = time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
		case <-sw.closedC:
			return
		case <-sw.ctx.Done():
			sw.closeWith(fmt.Errorf("oxia: client closed: %w", constant.ErrResourceUnavailable))
			return
		}

		sw.Lock()
		if sw.closed {
			sw.Unlock()
			return
		}
		expired := len(sw.inflight) > 0 && time.Since(sw.inflight[0].sentAt) > sw.requestTimeout
		pending := len(sw.inflight)
		sw.Unlock()

		if expired {
			slog.Warn("Write stream timed out; closing it",
				slog.Int64("shard", sw.shard),
				slog.Int("pendingWrites", pending),
				slog.Duration("requestTimeout", sw.requestTimeout))
			sw.closeWith(fmt.Errorf("oxia: write request timed out after %s: %w",
				sw.requestTimeout, context.DeadlineExceeded))
			return
		}
	}
}

// closeWith terminates the wrapper: every in-flight write fails with err
// and subsequent sends are refused. Idempotent; the first error wins.
func (sw *streamWrapper) closeWith(err error) {
	sw.Lock()
	if sw.closed {
		sw.Unlock()
		return
	}
	sw.closed = true
	toFail := sw.inflight
	sw.inflight = nil
	cancel := sw.streamCancel
	sw.stream = nil
	sw.streamCancel = nil
	sw.Unlock()

	close(sw.closedC)
	if cancel != nil {
		cancel()
	}
	for _, entry := range toFail {
		entry.future.Fail(err)
	}
}
