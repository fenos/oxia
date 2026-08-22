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
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/oxia-db/oxia/common/auth"
	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/common/rpc"
	commontime "github.com/oxia-db/oxia/common/time"
)

type RpcProvider interface {
	Executor
	io.Closer

	GetShardAssignments(context.Context, string, *proto.ShardAssignmentsRequest) (proto.OxiaClient_GetShardAssignmentsClient, error)
	GetSequenceUpdates(context.Context, string, *proto.GetSequenceUpdatesRequest) (proto.OxiaClient_GetSequenceUpdatesClient, error)
	GetNotifications(context.Context, string, *proto.NotificationsRequest) (proto.OxiaClient_GetNotificationsClient, error)
	CreateSession(context.Context, string, *proto.CreateSessionRequest) (*proto.CreateSessionResponse, error)
	KeepAlive(context.Context, string, *proto.SessionHeartbeat) (*proto.KeepAliveResponse, error)
	CloseSession(context.Context, string, *proto.CloseSessionRequest) (*proto.CloseSessionResponse, error)
}

type rpcProvider struct {
	clientPool           rpc.ClientPool
	serviceAddress       string
	shardManagerSupplier func() ShardManager
	writeStreamsMutex    sync.RWMutex
	writeStreams         map[int64]*streamWrapper
	requestTimeout       time.Duration

	ctx       context.Context
	namespace string
}

func NewRpcProvider(ctx context.Context, namespace string, tlsConf *tls.Config, authentication auth.Authentication,
	serviceAddress string, requestTimeout time.Duration, shardManagerSupplier func() ShardManager, dialOptions ...grpc.DialOption,
) RpcProvider {
	return &rpcProvider{
		ctx:                  ctx,
		namespace:            namespace,
		clientPool:           rpc.NewClientPool(tlsConf, authentication, dialOptions...),
		serviceAddress:       serviceAddress,
		requestTimeout:       requestTimeout,
		shardManagerSupplier: shardManagerSupplier,
		writeStreams:         make(map[int64]*streamWrapper),
	}
}

func (p *rpcProvider) Close() error {
	p.writeStreamsMutex.Lock()
	for shard, sw := range p.writeStreams {
		delete(p.writeStreams, shard)
		sw.closeWith(fmt.Errorf("oxia: client closed: %w", constant.ErrResourceUnavailable))
	}
	p.writeStreamsMutex.Unlock()

	return p.clientPool.Close()
}

func (p *rpcProvider) getClientByTarget(target string) (proto.OxiaClientClient, error) {
	client, err := p.clientPool.GetClientRpc(target)
	if err != nil {
		oxiaErr, _ := constant.FromGrpcError(err)
		return nil, oxiaErr
	}
	return client, nil
}

func (p *rpcProvider) getTargetByShard(shardId *int64, hint constant.ErrorMetadata) (string, error) {
	if shardId == nil {
		return p.serviceAddress, nil
	}

	shardManager := p.shardManagerSupplier()
	if shardManager == nil || !shardManager.Exists(*shardId) {
		return "", constant.ErrShardNotFound
	}

	if shard, leader, ok := hint.GetLeaderHint(); ok && shard == *shardId {
		return leader, nil
	}

	return shardManager.Leader(*shardId), nil
}

// getWriteStreamWrapper returns the shard's live stream wrapper, creating
// one — or replacing a closed one — on demand. The wrapper owns leader
// (re)resolution and stream recovery through the opener it is given.
func (p *rpcProvider) getWriteStreamWrapper(shardId int64) *streamWrapper {
	p.writeStreamsMutex.RLock()
	sw, ok := p.writeStreams[shardId]
	p.writeStreamsMutex.RUnlock()

	if ok && !sw.isClosed() {
		return sw
	}

	p.writeStreamsMutex.Lock()
	defer p.writeStreamsMutex.Unlock()

	if sw, ok = p.writeStreams[shardId]; ok && !sw.isClosed() {
		return sw
	}

	open := func(hint constant.ErrorMetadata) (proto.OxiaClient_WriteStreamClient, context.CancelFunc, error) {
		target, err := p.getTargetByShard(&shardId, hint)
		if err != nil {
			return nil, nil, err
		}
		streamCtx, cancel := context.WithCancel(p.ctx)
		streamCtx = metadata.AppendToOutgoingContext(streamCtx, constant.MetadataNamespace, p.namespace)
		streamCtx = metadata.AppendToOutgoingContext(streamCtx, constant.MetadataShardId, fmt.Sprintf("%d", shardId))
		stream, err := p.getWriteStream(streamCtx, target)
		if err != nil {
			cancel()
			return nil, nil, err
		}
		return stream, cancel, nil
	}

	sw = newStreamWrapper(p.ctx, shardId, open, p.requestTimeout)
	p.writeStreams[shardId] = sw
	return sw
}

func (p *rpcProvider) ExecuteWrite(ctx context.Context, request *proto.WriteRequest) (*proto.WriteResponse, error) {
	return executeWithRetry(ctx, func(constant.ErrorMetadata) (*proto.WriteResponse, error) {
		// Leader hints and stream recovery are the wrapper's business; a
		// closed wrapper surfaces a retryable error, and the retry finds
		// a fresh one here.
		return p.getWriteStreamWrapper(*request.Shard).Send(ctx, request) //nolint:contextcheck // Streams are tied to the client's lifetime context, not one request's.
	})
}

// ExecuteWriteAsync transmits the request without waiting for the
// response and returns a join that does the waiting. Requests transmitted
// by consecutive calls reach the shard in call order; the wrapper keeps
// that order across stream failures by replaying in-flight requests on
// the recovered stream.
func (p *rpcProvider) ExecuteWriteAsync(ctx context.Context, request *proto.WriteRequest) func() (*proto.WriteResponse, error) {
	f, err := p.getWriteStreamWrapper(*request.Shard).SendAsync(request) //nolint:contextcheck // Streams are tied to the client's lifetime context, not one request's.
	if err != nil {
		// The wrapper closed concurrently; a fresh one takes this send.
		f, err = p.getWriteStreamWrapper(*request.Shard).SendAsync(request) //nolint:contextcheck // See above.
	}
	if err != nil {
		return func() (*proto.WriteResponse, error) { return nil, err }
	}
	return func() (*proto.WriteResponse, error) { return f.Wait(ctx) }
}

func (p *rpcProvider) ExecuteRead(ctx context.Context, request *proto.ReadRequest) (*proto.ReadResponse, error) {
	return executeWithRetry(ctx, func(hint constant.ErrorMetadata) (*proto.ReadResponse, error) {
		target, err := p.getTargetByShard(request.Shard, hint)
		if err != nil {
			return nil, err
		}
		client, err := p.getClientByTarget(target)
		if err != nil {
			return nil, err
		}
		stream, err := client.Read(ctx, request)
		if err != nil {
			return nil, err
		}
		response := &proto.ReadResponse{}
		for {
			recv, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				return response, nil
			}
			if err != nil {
				return nil, err
			}
			response.Gets = append(response.Gets, recv.Gets...)
		}
	})
}

func (p *rpcProvider) ExecuteList(ctx context.Context, request *proto.ListRequest, listResponseConsumer func(*proto.ListResponse)) error {
	verified := false
	_, err := executeWithRetry(ctx, func(hint constant.ErrorMetadata) (struct{}, error) {
		target, err := p.getTargetByShard(request.Shard, hint)
		if err != nil {
			return struct{}{}, err
		}
		client, err := p.getClientByTarget(target)
		if err != nil {
			return struct{}{}, err
		}
		stream, err := client.List(ctx, request)
		if err != nil {
			oxiaErr, _ := constant.FromGrpcError(err)
			return struct{}{}, oxiaErr
		}
		for {
			response, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				return struct{}{}, nil
			}
			if err != nil {
				return struct{}{}, err
			}
			listResponseConsumer(response)
			verified = true
		}
	}, func(err error) bool {
		return !verified && constant.IsRetryable(err)
	})
	return err
}

func (p *rpcProvider) ExecuteRangeScan(ctx context.Context, request *proto.RangeScanRequest, rangeScanResponseConsumer func(*proto.RangeScanResponse)) error {
	verified := false
	_, err := executeWithRetry(ctx, func(hint constant.ErrorMetadata) (struct{}, error) {
		target, err := p.getTargetByShard(request.Shard, hint)
		if err != nil {
			return struct{}{}, err
		}
		client, err := p.getClientByTarget(target)
		if err != nil {
			return struct{}{}, err
		}
		stream, err := client.RangeScan(ctx, request)
		if err != nil {
			oxiaErr, _ := constant.FromGrpcError(err)
			return struct{}{}, oxiaErr
		}
		for {
			response, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				return struct{}{}, nil
			}
			if err != nil {
				return struct{}{}, err
			}
			rangeScanResponseConsumer(response)
			verified = true
		}
	}, func(err error) bool {
		return !verified && constant.IsRetryable(err)
	})
	return err
}

func (p *rpcProvider) GetShardAssignments(ctx context.Context, target string, request *proto.ShardAssignmentsRequest) (proto.OxiaClient_GetShardAssignmentsClient, error) {
	return executeWithRetry(ctx, func(constant.ErrorMetadata) (proto.OxiaClient_GetShardAssignmentsClient, error) {
		client, err := p.getClientByTarget(target)
		if err != nil {
			return nil, err
		}
		stream, err := client.GetShardAssignments(ctx, request)
		if err != nil {
			return nil, err
		}
		return &rpc.OxiaErrorServerStreamingClient[proto.ShardAssignments]{
			ServerStreamingClient: stream,
		}, nil
	})
}

func (p *rpcProvider) getWriteStream(ctx context.Context, target string) (proto.OxiaClient_WriteStreamClient, error) {
	client, err := p.getClientByTarget(target)
	if err != nil {
		return nil, err
	}
	return client.WriteStream(ctx)
}

func (p *rpcProvider) GetSequenceUpdates(ctx context.Context, target string, request *proto.GetSequenceUpdatesRequest) (proto.OxiaClient_GetSequenceUpdatesClient, error) {
	return executeWithRetry(ctx, func(constant.ErrorMetadata) (proto.OxiaClient_GetSequenceUpdatesClient, error) {
		client, err := p.getClientByTarget(target)
		if err != nil {
			return nil, err
		}
		stream, err := client.GetSequenceUpdates(ctx, request)
		if err != nil {
			return nil, err
		}
		return &rpc.OxiaErrorServerStreamingClient[proto.GetSequenceUpdatesResponse]{
			ServerStreamingClient: stream,
		}, nil
	})
}

func (p *rpcProvider) GetNotifications(ctx context.Context, target string, request *proto.NotificationsRequest) (proto.OxiaClient_GetNotificationsClient, error) {
	return executeWithRetry(ctx, func(constant.ErrorMetadata) (proto.OxiaClient_GetNotificationsClient, error) {
		client, err := p.getClientByTarget(target)
		if err != nil {
			return nil, err
		}
		stream, err := client.GetNotifications(ctx, request)
		if err != nil {
			return nil, err
		}
		return &rpc.OxiaErrorServerStreamingClient[proto.NotificationBatch]{
			ServerStreamingClient: stream,
		}, nil
	})
}

func (p *rpcProvider) CreateSession(ctx context.Context, target string, request *proto.CreateSessionRequest) (*proto.CreateSessionResponse, error) {
	return executeWithRetry(ctx, func(constant.ErrorMetadata) (*proto.CreateSessionResponse, error) {
		client, err := p.getClientByTarget(target)
		if err != nil {
			return nil, err
		}
		response, err := client.CreateSession(ctx, request)
		return response, err
	})
}

func (p *rpcProvider) KeepAlive(ctx context.Context, target string, request *proto.SessionHeartbeat) (*proto.KeepAliveResponse, error) {
	return executeWithRetry(ctx, func(constant.ErrorMetadata) (*proto.KeepAliveResponse, error) {
		client, err := p.getClientByTarget(target)
		if err != nil {
			return nil, err
		}
		response, err := client.KeepAlive(ctx, request)
		return response, err
	})
}

func (p *rpcProvider) CloseSession(ctx context.Context, target string, request *proto.CloseSessionRequest) (*proto.CloseSessionResponse, error) {
	return executeWithRetry(ctx, func(constant.ErrorMetadata) (*proto.CloseSessionResponse, error) {
		client, err := p.getClientByTarget(target)
		if err != nil {
			return nil, err
		}
		response, err := client.CloseSession(ctx, request)
		return response, err
	})
}

func executeWithRetry[T any](ctx context.Context, operation func(constant.ErrorMetadata) (T, error), isRetryable ...func(error) bool) (T, error) {
	var result T
	var hint constant.ErrorMetadata
	retryable := constant.IsRetryable
	if len(isRetryable) > 0 {
		retryable = isRetryable[0]
	}
	err := backoff.Retry(func() error {
		var err error
		result, err = operation(hint)
		if err == nil {
			return nil
		}
		var errorMetadata constant.ErrorMetadata
		err, errorMetadata = constant.FromGrpcError(err)
		if _, _, ok := errorMetadata.GetLeaderHint(); ok {
			hint = errorMetadata
		}
		if !retryable(err) {
			return backoff.Permanent(err)
		}
		return err
	}, commontime.NewBackOff(ctx))
	return result, err
}
