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

package client

import (
	"bytes"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/oxia"
	"github.com/oxia-db/oxia/oxiad/dataserver"
)

// goroutinesRunning reports whether any goroutine's stack mentions fn.
func goroutinesRunning(fn string) bool {
	var buf bytes.Buffer
	_ = pprof.Lookup("goroutine").WriteTo(&buf, 1)
	return strings.Contains(buf.String(), fn)
}

// Closing a client ends its shard-assignment stream: the goroutine that
// receives assignments and retries the stream must not outlive the client,
// or a closed client keeps retrying a closed connection forever.
func TestCloseStopsShardAssignmentReceiver(t *testing.T) {
	standalone, err := dataserver.NewStandalone(dataserver.NewTestConfig(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, standalone.Close()) })

	client, err := oxia.NewSyncClient(standalone.ServiceAddr())
	require.NoError(t, err)

	const receiver = "shardManagerImpl).receiveWithRecovery"
	require.True(t, goroutinesRunning(receiver), "the receiver runs while the client is open")

	require.NoError(t, client.Close())
	require.Eventually(t, func() bool { return !goroutinesRunning(receiver) },
		5*time.Second, 50*time.Millisecond, "the receiver ends with the client")
}
