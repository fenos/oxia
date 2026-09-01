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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia"
	"github.com/oxia-db/oxia/oxiad/dataserver"
)

func newStandalone(t *testing.T, namespaces ...dataserver.StandaloneNamespace) *dataserver.Standalone {
	t.Helper()

	config := dataserver.NewTestConfig(t.TempDir())
	config.Namespaces = namespaces

	standalone, err := dataserver.NewStandalone(config)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, standalone.Close())
	})

	return standalone
}

func newNamespaceClient(t *testing.T, addr, namespace string) oxia.SyncClient {
	t.Helper()

	client, err := oxia.NewSyncClient(addr, oxia.WithNamespace(namespace))
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, client.Close())
	})

	return client
}

func TestStandaloneNamespaces(t *testing.T) {
	standalone := newStandalone(t,
		dataserver.StandaloneNamespace{Name: "records", Shards: 2},
		dataserver.StandaloneNamespace{Name: "naming", Shards: 1, KeySorting: proto.KeySortingType_NATURAL},
	)

	records := newNamespaceClient(t, standalone.ServiceAddr(), "records")
	naming := newNamespaceClient(t, standalone.ServiceAddr(), "naming")

	_, _, err := records.Put(t.Context(), "k", []byte("in records"))
	require.NoError(t, err)
	_, _, err = naming.Put(t.Context(), "k", []byte("in naming"))
	require.NoError(t, err)

	_, value, _, err := records.Get(t.Context(), "k")
	require.NoError(t, err)
	assert.Equal(t, "in records", string(value))

	_, value, _, err = naming.Get(t.Context(), "k")
	require.NoError(t, err)
	assert.Equal(t, "in naming", string(value))
}

// Each namespace sorts keys its own way. Under the hierarchical order every
// key without a slash precedes every key with one, so "b" comes before
// "a/b"; under the natural (bytewise) order "a/b" comes before "b". The
// list ranges differ for the same reason: a hierarchical range that holds
// "a/b" must end at a slash key.
func TestStandaloneNamespaceKeySorting(t *testing.T) {
	standalone := newStandalone(t,
		dataserver.StandaloneNamespace{Name: "hierarchical", Shards: 1, KeySorting: proto.KeySortingType_HIERARCHICAL},
		dataserver.StandaloneNamespace{Name: "natural", Shards: 1, KeySorting: proto.KeySortingType_NATURAL},
	)

	for _, namespace := range []string{"hierarchical", "natural"} {
		client := newNamespaceClient(t, standalone.ServiceAddr(), namespace)
		for _, key := range []string{"b", "a/b"} {
			_, _, err := client.Put(t.Context(), key, []byte(key))
			require.NoError(t, err)
		}
	}

	keys, err := newNamespaceClient(t, standalone.ServiceAddr(), "hierarchical").List(t.Context(), "a", "a/c")
	require.NoError(t, err)
	assert.Equal(t, []string{"b", "a/b"}, keys)

	keys, err = newNamespaceClient(t, standalone.ServiceAddr(), "natural").List(t.Context(), "a", "c")
	require.NoError(t, err)
	assert.Equal(t, []string{"a/b", "b"}, keys)
}

// Shard IDs are allocated from one counter across namespaces in the order
// they are listed — the same scheme a coordinator uses — so a standalone
// data directory is a valid cluster layout.
func TestStandaloneShardIDsFollowNamespaceOrder(t *testing.T) {
	standalone := newStandalone(t,
		dataserver.StandaloneNamespace{Name: "records", Shards: 2},
		dataserver.StandaloneNamespace{Name: "naming", Shards: 1},
	)

	conn, err := grpc.NewClient(standalone.ServiceAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, conn.Close())
	})

	shardIDs := func(namespace string) []int64 {
		stream, err := proto.NewOxiaClientClient(conn).GetShardAssignments(t.Context(),
			&proto.ShardAssignmentsRequest{Namespace: namespace})
		require.NoError(t, err)
		assignments, err := stream.Recv()
		require.NoError(t, err)

		shards := assignments.GetNamespaces()[namespace].GetAssignments()
		ids := make([]int64, 0, len(shards))
		for _, a := range shards {
			ids = append(ids, a.GetShard())
		}
		return ids
	}

	assert.ElementsMatch(t, []int64{0, 1}, shardIDs("records"))
	assert.ElementsMatch(t, []int64{2}, shardIDs("naming"))
}
