// Copyright 2025 StreamNative, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metadata

import (
	"testing"

	"github.com/stretchr/testify/require"

	commonproto "github.com/oxia-db/oxia/common/proto"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
)

// conflictedProvider always loses the version race — the shape of a
// brief leadership overlap, where another writer moved the document on.
type conflictedProvider struct {
	watch *commonwatch.Watch[provider.Versioned[*commonproto.ClusterStatus]]
}

func (p *conflictedProvider) Store(provider.Versioned[*commonproto.ClusterStatus]) (metadatacommon.Version, error) {
	return "", metadatacommon.ErrBadVersion
}

func (p *conflictedProvider) WaitToBecomeLeader() (<-chan struct{}, error) {
	return make(chan struct{}), nil
}
func (p *conflictedProvider) GetLeaderName() (string, error) { return "", nil }
func (p *conflictedProvider) Watch() *commonwatch.Watch[provider.Versioned[*commonproto.ClusterStatus]] {
	return p.watch
}
func (p *conflictedProvider) Close() error { return nil }

// A lost version race returns to the caller — whose retries run over
// fresh state — and never panics: with the coordinator embedded, a
// panic here took the whole embedding process down and flapped the
// leadership it was racing.
func TestComputeStatusSurvivesALostVersionRace(t *testing.T) {
	watch := commonwatch.New(provider.Versioned[*commonproto.ClusterStatus]{Value: &commonproto.ClusterStatus{}, Version: "1"})
	m := &coordinatorMetadata{statusProvider: &conflictedProvider{watch: watch}}

	err := m.computeStatus(func(status *commonproto.ClusterStatus, _ metadatacommon.Version) (*commonproto.ClusterStatus, bool) {
		return status, true
	})
	require.ErrorIs(t, err, metadatacommon.ErrBadVersion)
}
