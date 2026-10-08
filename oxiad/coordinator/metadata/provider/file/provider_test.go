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

package file

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	commonproto "github.com/oxia-db/oxia/common/proto"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	metadatacodec "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common/codec"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
)

type statusProvider = provider.Provider[*commonproto.ClusterStatus]

func newTestProvider(t *testing.T, path string) statusProvider {
	t.Helper()

	p, err := NewProvider(t.Context(), path, metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, "test")
	require.NoError(t, err)
	return p
}

func startWaitToBecomeLeader(p statusProvider) <-chan error {
	acquired := make(chan error, 1)
	go func() {
		_, err := p.WaitToBecomeLeader()
		acquired <- err
	}()
	return acquired
}

func requireStillWaiting(t *testing.T, acquired <-chan error) {
	t.Helper()

	select {
	case err := <-acquired:
		require.FailNow(t, "became leader while the lock was held", "err: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
}

func requireReturns(t *testing.T, acquired <-chan error) error {
	t.Helper()

	select {
	case err := <-acquired:
		return err
	case <-time.After(10 * time.Second):
		require.FailNow(t, "the wait for the leadership did not end")
		return nil
	}
}

// A coordinator waiting for the leadership while another holds it can be
// stopped: closing its provider ends the wait with an error, and leaves the
// leader's lock in place.
func TestCloseEndsWaitToBecomeLeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata")

	leader := newTestProvider(t, path)
	_, err := leader.WaitToBecomeLeader()
	require.NoError(t, err)

	waiting := newTestProvider(t, path)
	acquired := startWaitToBecomeLeader(waiting)
	requireStillWaiting(t, acquired)

	require.NoError(t, waiting.Close())
	require.ErrorIs(t, requireReturns(t, acquired), context.Canceled)

	follower := newTestProvider(t, path)
	acquired = startWaitToBecomeLeader(follower)
	requireStillWaiting(t, acquired)

	require.NoError(t, leader.Close())
	require.NoError(t, requireReturns(t, acquired))
	require.NoError(t, follower.Close())
}
