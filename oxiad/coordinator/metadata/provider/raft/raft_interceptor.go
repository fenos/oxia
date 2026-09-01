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

package raft

import "sync"

type Interceptor interface {
	OnApplied(key string, data []byte, version int64)
}

// Interceptors fans every applied entry out to several interceptors: the
// providers of one node, which are created after the node itself and added
// here once they exist. Safe to add to while entries are being applied.
type Interceptors struct {
	mu   sync.RWMutex
	list []Interceptor
}

// Add registers interceptors; later entries reach them too.
func (i *Interceptors) Add(interceptors ...Interceptor) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.list = append(i.list, interceptors...)
}

func (i *Interceptors) OnApplied(key string, data []byte, version int64) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	for _, interceptor := range i.list {
		interceptor.OnApplied(key, data, version)
	}
}
