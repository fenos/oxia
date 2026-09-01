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

package embedded

import (
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia"
	"github.com/oxia-db/oxia/oxiad/coordinator"
	coordinatoroption "github.com/oxia-db/oxia/oxiad/coordinator/option"
	"github.com/oxia-db/oxia/oxiad/dataserver"
	dataserveroption "github.com/oxia-db/oxia/oxiad/dataserver/option"
)

// A standalone data directory becomes a coordinated cluster's. Two shapes
// of the same adoption: after one standalone boot the coordinator's first
// attempt is exactly the term standalone led under — which the log holds
// entries of — and the election must move above it rather than lead twice
// under one term; after twenty boots the standalone sits many terms ahead
// and the rejecting server reports its term, so the election moves there
// in one round instead of counting up. Either way the shard is led one
// term above standalone's last, and a second coordinated boot of the same
// directory serves what the first wrote — a term led under twice leaves
// the store unopenable.
func TestCoordinatorAdoptsAStandaloneDirectory(t *testing.T) {
	for name, boots := range map[string]int{"after one boot": 1, "after twenty boots": 20} {
		t.Run(name, func(t *testing.T) {
			testAdoption(t, boots)
		})
	}
}

func testAdoption(t *testing.T, boots int) {
	t.Helper()

	const namespace = "records"
	dir := t.TempDir()
	// Fixed ports: the coordinator's configuration keeps the data server's
	// address, so the second boot must come up where the first did.
	public, internal := freeLocalAddress(t), freeLocalAddress(t)

	for boot := range boots {
		standalone, err := dataserver.NewStandalone(dataserver.StandaloneConfig{
			DataServerOptions: *dataServerOptionsIn(dir, "localhost:0", "localhost:0"),
			Namespaces:        []dataserver.StandaloneNamespace{{Name: namespace, Shards: 1}},
		})
		require.NoError(t, err)
		if boot == 0 {
			client := newNamespaceClient(t, standalone.ServiceAddr(), namespace)
			_, _, err := client.Put(t.Context(), "k", []byte("written standalone"))
			require.NoError(t, err)
			require.NoError(t, client.Close())
		}
		require.NoError(t, standalone.Close())
	}

	// The coordinator keeps its metadata in a file: the second boot is the
	// same coordinator instance to the data server.
	options := coordinatoroption.NewDefaultOptions()
	options.Server.Public.BindAddress = "localhost:0"
	options.Server.Internal.BindAddress = "localhost:0"
	options.Observability.Metric.Enabled = &constant.FlagFalse
	options.Metadata.Name = "adopting-coordinator"
	options.Metadata.ProviderName = coordinatoroption.ProviderFile
	options.Metadata.File.Dir = filepath.Join(dir, "coordinator")
	identity := &proto.DataServerIdentity{Public: public, Internal: internal}

	boot := func(round string) (oxia.SyncClient, *coordinator.GrpcServer, func()) {
		server, err := dataserver.New(t.Context(), dataServerOptionsIn(dir, public, internal))
		require.NoError(t, err, round)

		started := time.Now()
		coord, err := coordinator.New(t.Context(), options,
			coordinator.WithInitialClusterConfiguration(&proto.ClusterConfiguration{
				Namespaces: []*proto.Namespace{{Name: namespace, ReplicationFactor: 1, InitialShardCount: 1}},
				Servers:    []*proto.DataServerIdentity{identity},
			}))
		if err != nil {
			assert.NoError(t, server.Close())
			require.NoError(t, err, round)
		}

		client, err := oxia.NewSyncClient(identity.Public, oxia.WithNamespace(namespace))
		if err != nil {
			assert.NoError(t, coord.Close())
			assert.NoError(t, server.Close())
			require.NoError(t, err, round)
		}
		// The bound leaves room for the coordinator's first assignment
		// stream, which can take a ten-second cycle to reach a server that
		// came up in the same second; the election itself is over in one
		// extra round.
		assert.Less(t, time.Since(started), 30*time.Second, "%s: adoption takes one extra election round, not one per standalone boot", round)

		var once sync.Once
		stop := func() {
			once.Do(func() {
				assert.NoError(t, client.Close())
				assert.NoError(t, coord.Close())
				assert.NoError(t, server.Close())
			})
		}
		t.Cleanup(stop)

		return client, coord, stop
	}

	client, coord, stop := boot("first coordinated boot")
	_, value, _, err := client.Get(t.Context(), "k")
	require.NoError(t, err)
	assert.Equal(t, "written standalone", string(value))
	assert.EqualValues(t, boots, shardTerm(t, coord, namespace),
		"standalone led terms 0..%d; the coordinator leads one above", boots-1)
	_, _, err = client.Put(t.Context(), "k", []byte("written coordinated"))
	require.NoError(t, err)
	stop()

	client, _, stop = boot("second coordinated boot")
	_, value, _, err = client.Get(t.Context(), "k")
	require.NoError(t, err)
	assert.Equal(t, "written coordinated", string(value))
	stop()
}

// shardTerm reads a namespace's only shard's term off the coordinator's
// admin API.
func shardTerm(t *testing.T, coord *coordinator.GrpcServer, namespace string) int64 {
	t.Helper()

	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", coord.PublicPort()), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, conn.Close()) })

	view, err := proto.NewOxiaAdminClient(conn).GetNamespace(t.Context(), &proto.GetNamespaceRequest{Namespace: namespace})
	require.NoError(t, err)
	shards := view.GetNamespace().GetNamespaceStatus().GetShards()
	require.Len(t, shards, 1)
	for _, shard := range shards {
		return shard.GetTerm()
	}
	return -1
}

func freeLocalAddress(t *testing.T) string {
	t.Helper()

	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "localhost:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	require.NoError(t, listener.Close())
	return addr
}

// dataServerOptionsIn is a data server rooted at dir, bound as told,
// metrics off — the same shape whether it runs standalone or coordinated.
func dataServerOptionsIn(dir, public, internal string) *dataserveroption.Options {
	opts := dataserveroption.NewDefaultOptions()
	opts.Server.Public.BindAddress = public
	opts.Server.Internal.BindAddress = internal
	opts.Observability.Metric.Enabled = &constant.FlagFalse
	opts.Storage.Database.Dir = filepath.Join(dir, "db")
	opts.Storage.WAL.Dir = filepath.Join(dir, "wal")
	return opts
}
