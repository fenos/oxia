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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia"
	"github.com/oxia-db/oxia/oxiad/coordinator"
	coordinatoroption "github.com/oxia-db/oxia/oxiad/coordinator/option"
)

// Raising a namespace's replication factor reaches its shards: every
// ensemble grows, one member per balancer cycle, until it has the new
// factor — and what was written at the old factor stays readable.
func TestRaisedReplicationFactorGrowsEnsembles(t *testing.T) {
	const namespace = "records"

	_, id1 := newDataServer(t)
	_, id2 := newDataServer(t)
	_, id3 := newDataServer(t)

	options := coordinatoroption.NewDefaultOptions()
	options.Server.Public.BindAddress = "localhost:0"
	options.Server.Internal.BindAddress = "localhost:0"
	options.Observability.Metric.Enabled = &constant.FlagFalse
	options.Metadata.Name = "growing-coordinator"
	options.Metadata.ProviderName = coordinatoroption.ProviderMemory

	coord, err := coordinator.New(t.Context(), options,
		coordinator.WithInitialClusterConfiguration(&proto.ClusterConfiguration{
			Namespaces: []*proto.Namespace{{Name: namespace, ReplicationFactor: 1, InitialShardCount: 2}},
			Servers:    []*proto.DataServerIdentity{id1, id2, id3},
		}))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, coord.Close()) })

	client, err := oxia.NewSyncClient(id1.Public, oxia.WithNamespace(namespace))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, client.Close()) })
	for i := range 10 {
		_, _, err := client.Put(t.Context(), fmt.Sprintf("key-%d", i), fmt.Appendf(nil, "value-%d", i))
		require.NoError(t, err)
	}

	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", coord.PublicPort()), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, conn.Close()) })
	admin := proto.NewOxiaAdminClient(conn)

	_, err = admin.PatchNamespace(t.Context(), &proto.PatchNamespaceRequest{
		Namespace: &proto.Namespace{Name: namespace, ReplicationFactor: 3},
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		view, err := admin.GetNamespace(t.Context(), &proto.GetNamespaceRequest{Namespace: namespace})
		if err != nil {
			return false
		}
		for _, shard := range view.GetNamespace().GetNamespaceStatus().GetShards() {
			if len(shard.GetEnsemble()) != 3 || shard.GetStatus() != proto.ShardStatusSteadyState {
				return false
			}
		}
		return true
	}, 90*time.Second, 500*time.Millisecond, "every shard's ensemble reaches the raised factor")

	for i := range 10 {
		_, value, _, err := client.Get(t.Context(), fmt.Sprintf("key-%d", i))
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("value-%d", i), string(value))
	}
}
