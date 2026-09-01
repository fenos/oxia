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

package coordinator

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/option"
)

func TestServerOptions(t *testing.T) {
	so := newServerOptions(nil)
	assert.NotNil(t, so.onLeadershipLost)
	assert.Nil(t, so.initialClusterConfig)

	config := &proto.ClusterConfiguration{}
	called := false
	so = newServerOptions([]ServerOption{
		WithOnLeadershipLost(func() { called = true }),
		WithInitialClusterConfiguration(config),
	})

	so.onLeadershipLost()
	assert.True(t, called)
	assert.Same(t, config, so.initialClusterConfig)
}

// A nil option is ignored and a nil leadership-loss handler keeps the
// fail-safe default: neither can leave the coordinator with a nil to call.
func TestServerOptionsNilSafe(t *testing.T) {
	so := newServerOptions([]ServerOption{nil, WithOnLeadershipLost(nil)})
	assert.NotNil(t, so.onLeadershipLost)
	assert.Nil(t, so.initialClusterConfig)
}

// A coordinator that is not the founder of its raft group waits to be
// elected for as long as another leads — possibly forever. Cancelling the
// start's context ends that wait with an error instead of a hang.
func TestNewReturnsWhenStartIsCancelledWhileWaitingForLeadership(t *testing.T) {
	freeAddress := func() string {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		addr := listener.Addr().String()
		require.NoError(t, listener.Close())
		return addr
	}
	founder, self := freeAddress(), freeAddress()

	options := option.NewDefaultOptions()
	options.Server.Public.BindAddress = "127.0.0.1:0"
	options.Server.Internal.BindAddress = "127.0.0.1:0"
	options.Observability.Metric.Enabled = &constant.FlagFalse
	options.Metadata.ProviderName = option.ProviderRaft
	options.Metadata.Name = self
	options.Metadata.Raft.Address = self
	options.Metadata.Raft.Peers = []string{founder, self} // the founder never starts
	options.Metadata.Raft.DataDir = filepath.Join(t.TempDir(), "raft")

	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		server, err := New(ctx, options)
		if err == nil {
			err = server.Close()
		}
		done <- err
	}()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(30 * time.Second):
		t.Fatal("New did not return after its context was cancelled")
	}
}
