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

package codec

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	commonproto "github.com/oxia-db/oxia/common/proto"
)

func TestStatusCodecUnmarshalYAMLRejectsInvalidYAML(t *testing.T) {
	status, err := ClusterStatusCodec.UnmarshalYAML([]byte("namespaces: [unterminated"))
	require.Error(t, err)
	require.Nil(t, status)
}

func TestStatusCodecUnmarshalJSONRejectsInvalidJSON(t *testing.T) {
	status, err := ClusterStatusCodec.UnmarshalJSON([]byte(`{"namespaces": "not a map"}`))
	require.Error(t, err)
	require.Nil(t, status)
}

// The status renders to YAML while other goroutines use the data servers it
// holds, e.g. to log them: a message renders the first time by storing its
// state, which the YAML encoder must not read.
func TestStatusCodecMarshalYAMLWhileTheDataServersAreLogged(t *testing.T) {
	identity := &commonproto.DataServerIdentity{Public: "s1:6648", Internal: "s1:6649"}
	status := &commonproto.ClusterStatus{Namespaces: map[string]*commonproto.NamespaceStatus{
		"default": {Shards: map[int64]*commonproto.ShardMetadata{0: {Ensemble: []*commonproto.DataServerIdentity{identity}}}},
	}}

	var wg sync.WaitGroup
	wg.Go(func() { _ = identity.String() })
	data, err := ClusterStatusCodec.MarshalYAML(status)
	wg.Wait()

	require.NoError(t, err)
	decoded, err := ClusterStatusCodec.UnmarshalYAML(data)
	require.NoError(t, err)
	require.Equal(t, "s1:6648", decoded.GetNamespaces()["default"].GetShards()[0].GetEnsemble()[0].GetPublic())
}
