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

func TestConfigCodecUnmarshalYAMLRejectsInvalidYAML(t *testing.T) {
	config, err := ClusterConfigCodec.UnmarshalYAML([]byte("servers: [unterminated"))
	require.Error(t, err)
	require.Nil(t, config)
}

func TestConfigCodecUnmarshalJSONRejectsInvalidJSON(t *testing.T) {
	config, err := ClusterConfigCodec.UnmarshalJSON([]byte(`{"servers": "not a list"}`))
	require.Error(t, err)
	require.Nil(t, config)
}

// The configuration renders to YAML while other goroutines use the data
// servers it holds, e.g. to log them.
func TestConfigCodecMarshalYAMLWhileTheDataServersAreLogged(t *testing.T) {
	identity := &commonproto.DataServerIdentity{Public: "s1:6648", Internal: "s1:6649"}
	config := &commonproto.ClusterConfiguration{Servers: []*commonproto.DataServerIdentity{identity}}

	var wg sync.WaitGroup
	wg.Go(func() { _ = identity.String() })
	data, err := ClusterConfigCodec.MarshalYAML(config)
	wg.Wait()

	require.NoError(t, err)
	decoded, err := ClusterConfigCodec.UnmarshalYAML(data)
	require.NoError(t, err)
	require.Equal(t, "s1:6648", decoded.GetServers()[0].GetPublic())
}
