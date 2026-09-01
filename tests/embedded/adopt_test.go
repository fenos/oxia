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
	"path/filepath"
	"testing"
	"time"

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

// A standalone data directory becomes a coordinated cluster's: the
// coordinator, starting from an empty status, adopts shards that are many
// terms ahead within seconds — the rejecting server reports its term and
// the election moves there, instead of counting up through every term the
// standalone ever ran.
func TestCoordinatorAdoptsAStandaloneDirectory(t *testing.T) {
	// Twenty boots put the standalone at term 20: counting up through them
	// with the election's backoff would take minutes; one reported term
	// takes one round.
	const namespace, boots = "records", 20
	dir := t.TempDir()

	standaloneOptions := func() dataserveroption.Options {
		opts := dataServerOptionsIn(dir)
		return *opts
	}
	for boot := range boots {
		standalone, err := dataserver.NewStandalone(dataserver.StandaloneConfig{
			DataServerOptions: standaloneOptions(),
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

	server, err := dataserver.New(t.Context(), dataServerOptionsIn(dir))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, server.Close()) })
	identity := &proto.DataServerIdentity{
		Public:   fmt.Sprintf("localhost:%d", server.PublicPort()),
		Internal: fmt.Sprintf("localhost:%d", server.InternalPort()),
	}

	options := coordinatoroption.NewDefaultOptions()
	options.Server.Public.BindAddress = "localhost:0"
	options.Server.Internal.BindAddress = "localhost:0"
	options.Observability.Metric.Enabled = &constant.FlagFalse
	options.Metadata.Name = "adopting-coordinator"
	options.Metadata.ProviderName = coordinatoroption.ProviderMemory

	adopted := time.Now()
	coord, err := coordinator.New(t.Context(), options,
		coordinator.WithInitialClusterConfiguration(&proto.ClusterConfiguration{
			Namespaces: []*proto.Namespace{{Name: namespace, ReplicationFactor: 1, InitialShardCount: 1}},
			Servers:    []*proto.DataServerIdentity{identity},
		}))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, coord.Close()) })

	client, err := oxia.NewSyncClient(identity.Public, oxia.WithNamespace(namespace))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, client.Close()) })

	_, value, _, err := client.Get(t.Context(), "k")
	require.NoError(t, err)
	assert.Equal(t, "written standalone", string(value))
	// The bound leaves room for the coordinator's first assignment stream,
	// which can take a ten-second cycle to reach a server that came up in
	// the same second; the election itself is over in one extra round.
	assert.Less(t, time.Since(adopted), 30*time.Second,
		"adoption takes one extra election round, not one per standalone boot")
}

// dataServerOptionsIn is a data server rooted at dir, on ephemeral ports,
// metrics off — the same shape whether it runs standalone or coordinated.
func dataServerOptionsIn(dir string) *dataserveroption.Options {
	opts := dataserveroption.NewDefaultOptions()
	opts.Server.Public.BindAddress = "localhost:0"
	opts.Server.Internal.BindAddress = "localhost:0"
	opts.Observability.Metric.Enabled = &constant.FlagFalse
	opts.Storage.Database.Dir = filepath.Join(dir, "db")
	opts.Storage.WAL.Dir = filepath.Join(dir, "wal")
	return opts
}
