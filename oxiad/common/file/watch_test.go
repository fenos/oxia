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
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/failpoint"
)

func TestWatchFileSignalsWriteAndClosesOnCancel(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changes, err := WatchFile(ctx, path)
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(path, []byte("a: 1"), 0o600))
	select {
	case _, ok := <-changes:
		assert.True(t, ok)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "no change signalled for the written file")
	}

	cancel()
	assert.Eventually(t, func() bool {
		select {
		case _, ok := <-changes:
			return !ok
		default:
			return false
		}
	}, 10*time.Second, 10*time.Millisecond)
}

func TestWatchFileReturnsErrorForMissingDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing", "config.yaml")

	changes, err := WatchFile(context.Background(), path)

	assert.Error(t, err)
	assert.Nil(t, changes)
}

func TestWatchFileReturnsWatcherCreationError(t *testing.T) {
	failWatcherCreation(t)

	changes, err := WatchFile(t.Context(), filepath.Join(t.TempDir(), "config.yaml"))

	assert.ErrorIs(t, err, errWatcherCreation)
	assert.Nil(t, changes)
}

// A Kubernetes ConfigMap volume updates a file by swapping the link of its
// directory: the file's own path sees no event.
func TestWatchFileSignalsWhenThePathLinksToAnotherFile(t *testing.T) {
	dir := t.TempDir()
	writeVersion := func(version string) {
		require.NoError(t, os.Mkdir(filepath.Join(dir, version), 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(dir, version, "config.yaml"), []byte(version), 0o600))
	}
	writeVersion("..v1")
	require.NoError(t, os.Symlink("..v1", filepath.Join(dir, "..data")))
	path := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.Symlink(filepath.Join("..data", "config.yaml"), path))

	changes, err := WatchFile(t.Context(), path)
	require.NoError(t, err)

	writeVersion("..v2")
	require.NoError(t, os.Symlink("..v2", filepath.Join(dir, "..data_tmp")))
	require.NoError(t, os.Rename(filepath.Join(dir, "..data_tmp"), filepath.Join(dir, "..data")))
	select {
	case _, ok := <-changes:
		assert.True(t, ok)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "no change signalled for the swapped link")
	}
	content, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, "..v2", string(content))
}

func TestReloadOnChangeReloadsTheChangedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	reloads := make(chan struct{}, 1)

	ReloadOnChange(t.Context(), path, func() {
		select {
		case reloads <- struct{}{}:
		default:
		}
	})

	require.NoError(t, os.WriteFile(path, []byte("a: 1"), 0o600))
	select {
	case <-reloads:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "the changed file was not reloaded")
	}
}

// A host out of inotify instances fails the creation of the watcher: the
// process runs on, without the reloads.
func TestReloadOnChangeRunsOnWithoutAWatch(t *testing.T) {
	failWatcherCreation(t)

	assert.NotPanics(t, func() {
		ReloadOnChange(t.Context(), filepath.Join(t.TempDir(), "config.yaml"), func() {
			assert.Fail(t, "reloaded without a watch")
		})
	})
}

// The watcher reports the events of the directory in an order that depends
// on the platform: fileChanged decides on each one from the links as they
// are.
func TestFileChangedSignalsAMoveBetweenResolvedFiles(t *testing.T) {
	dir := t.TempDir()
	for _, version := range []string{"..v1", "..v2"} {
		require.NoError(t, os.Mkdir(filepath.Join(dir, version), 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(dir, version, "config.yaml"), []byte(version), 0o600))
	}
	require.NoError(t, os.Symlink("..v1", filepath.Join(dir, "..data")))
	path := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.Symlink(filepath.Join("..data", "config.yaml"), path))
	v1 := linkTarget(path)
	require.NotEmpty(t, v1)
	other := fsnotify.Event{Name: filepath.Join(dir, "..data"), Op: fsnotify.Create}

	target, changed := fileChanged(other, path, v1)
	assert.False(t, changed, "the path still resolves to the same file")
	assert.Equal(t, v1, target)

	require.NoError(t, os.Remove(filepath.Join(dir, "..data")))
	require.NoError(t, os.Symlink("..v2", filepath.Join(dir, "..data")))
	target, changed = fileChanged(other, path, v1)
	assert.True(t, changed, "the path resolves to another file")
	assert.Equal(t, linkTarget(path), target)
	assert.NotEqual(t, v1, target)

	target, changed = fileChanged(fsnotify.Event{Name: path, Op: fsnotify.Write}, path, target)
	assert.True(t, changed, "the file itself changed")
	assert.Equal(t, linkTarget(path), target)
}

func TestWatchFileClosesWhenTheWatchFails(t *testing.T) {
	skipWithoutFailpoints(t)
	path := filepath.Join(t.TempDir(), "config.yaml")
	failpoints := failpoint.New(map[string]func() error{
		"watchFile.event": func() error { return errors.New("injected") },
	})

	changes, err := watchFile(t.Context(), path, failpoints)
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(path, []byte("a: 1"), 0o600))
	assertClosed(t, changes)
}

func TestWatchFileClosesWhenTheWatcherFails(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	paused := make(chan struct{})
	release := make(chan struct{})
	var pause sync.Once
	failpoints := failpoint.New(map[string]func() error{
		"watchFile.event": func() error {
			pause.Do(func() {
				close(paused)
				<-release
			})
			return nil
		},
	})

	changes, err := watchFile(t.Context(), path, failpoints)
	require.NoError(t, err)

	switch runtime.GOOS {
	case "linux":
		// inotify reports an overflow once the events that nobody reads pass
		// its queue limit
		require.NoError(t, os.WriteFile(filepath.Join(dir, "first"), nil, 0o600))
		select {
		case <-paused:
		case <-time.After(10 * time.Second):
			require.FailNow(t, "no event for the written file")
		}
		data, err := os.ReadFile("/proc/sys/fs/inotify/max_queued_events")
		require.NoError(t, err)
		limit, err := strconv.Atoi(strings.TrimSpace(string(data)))
		require.NoError(t, err)
		// The watcher reads a batch of the events from the queue before it
		// waits for the watch to take them
		for i := range limit + 4096 {
			require.NoError(t, os.WriteFile(filepath.Join(dir, strconv.Itoa(i)), nil, 0o600))
		}
	case "darwin", "dragonfly", "freebsd", "netbsd", "openbsd":
		// kqueue reports the failure to list a directory whose content changed
		require.NoError(t, os.Chmod(dir, 0o300))
		t.Cleanup(func() { assert.NoError(t, os.Chmod(dir, 0o700)) })
		require.NoError(t, os.WriteFile(path, nil, 0o600))
	default:
		t.Skipf("no way to fail the watcher on %s", runtime.GOOS)
	}
	close(release)

	assertClosed(t, changes)
}

func TestWatchFileClosesWhenTheWatcherFailsToClose(t *testing.T) {
	skipWithoutFailpoints(t)
	path := filepath.Join(t.TempDir(), "config.yaml")
	ctx, cancel := context.WithCancel(t.Context())
	closed := make(chan struct{})
	failpoints := failpoint.New(map[string]func() error{
		"watchFile.close": func() error {
			close(closed)
			return errors.New("injected")
		},
	})

	changes, err := watchFile(ctx, path, failpoints)
	require.NoError(t, err)

	cancel()
	assertClosed(t, changes)
	select {
	case <-closed:
	default:
		assert.Fail(t, "the watcher was not closed")
	}
}

func assertClosed(t *testing.T, changes <-chan struct{}) {
	t.Helper()
	select {
	case _, ok := <-changes:
		assert.False(t, ok)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "the changes channel was not closed")
	}
}

var errWatcherCreation = errors.New("too many open files")

// failWatcherCreation fails the creation of the file watchers of the process
// with errWatcherCreation until the test ends.
func failWatcherCreation(t *testing.T) {
	t.Helper()
	skipWithoutFailpoints(t)
	failpoint.Enable("watchFile.newWatcher", errWatcherCreation)
	t.Cleanup(func() { failpoint.Disable("watchFile.newWatcher") })
}

func skipWithoutFailpoints(t *testing.T) {
	t.Helper()
	if !failpoint.Enabled {
		t.Skip("needs failpoints: disabled by the disable_trap tag")
	}
}
