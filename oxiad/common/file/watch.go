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
	"log/slog"
	"path/filepath"

	"github.com/fsnotify/fsnotify"

	"github.com/oxia-db/oxia/common/channel"
	"github.com/oxia-db/oxia/common/failpoint"
)

// WatchFile signals each time the file at path is created, written, renamed
// or removed, and each time path resolves to another file through its
// symbolic links, as when Kubernetes updates a ConfigMap volume. The channel
// is closed when the watch fails or ctx is done.
func WatchFile(ctx context.Context, path string) (<-chan struct{}, error) {
	return watchFile(ctx, path, nil)
}

// ReloadOnChange calls reload each time the file at path changes, until ctx is
// done. A file that can't be watched, e.g. on a host out of inotify instances,
// is only read again at the next start: ReloadOnChange logs a warning and
// returns, and the process runs on.
func ReloadOnChange(ctx context.Context, path string, reload func()) {
	changes, err := WatchFile(ctx, path)
	if err != nil {
		slog.Warn("Failed to watch the file: its changes apply at the next start",
			slog.String("path", path), slog.Any("error", err))
		return
	}
	go func() {
		for range changes {
			reload()
		}
	}()
}

func watchFile(ctx context.Context, path string, failpoints *failpoint.Failpoints) (<-chan struct{}, error) {
	logger := slog.With(slog.String("component", "file-watch"), slog.String("path", path))
	watcher, err := fsnotify.NewWatcher()
	if failErr, ok := failpoint.Eval[error]("watchFile.newWatcher"); ok {
		err = errors.Join(err, failErr)
	}
	if err != nil {
		if watcher != nil {
			err = errors.Join(err, watcher.Close())
		}
		return nil, err
	}
	if err := watcher.Add(filepath.Dir(path)); err != nil {
		return nil, errors.Join(err, watcher.Close())
	}

	changes := make(chan struct{}, 1)
	go func() {
		defer close(changes)
		defer func() {
			if err := failpoints.Fail("watchFile.close", watcher.Close()); err != nil {
				logger.Warn("Failed to close file watcher", slog.Any("error", err))
			}
		}()
		target := linkTarget(path)
		// The watcher closes its channels only once it is closed, when this
		// loop has returned
		for {
			var err error
			select {
			case <-ctx.Done():
				return
			case err = <-watcher.Errors:
			case event := <-watcher.Events:
				if err = failpoints.Trigger("watchFile.event"); err == nil {
					var changed bool
					if target, changed = fileChanged(event, path, target); changed {
						channel.PushNoBlock(changes, struct{}{})
					}
					continue
				}
			}
			logger.Warn("File watch failed", slog.Any("error", err))
			return
		}
	}()
	return changes, nil
}

// fileChanged reports whether event changed the file at path, which resolved
// to target through its symbolic links, and returns the file that path
// resolves to now.
func fileChanged(event fsnotify.Event, path string, target string) (string, bool) {
	current := linkTarget(path)
	if filepath.Clean(event.Name) == filepath.Clean(path) &&
		event.Op&(fsnotify.Create|fsnotify.Write|fsnotify.Rename|fsnotify.Remove) != 0 {
		return current, true
	}
	// A ConfigMap volume moves the path from one file to another. Its own
	// events tell when the file appears or goes
	if current != "" && target != "" && current != target {
		return current, true
	}
	return target, false
}

// linkTarget returns the file that path resolves to through its symbolic
// links, or "" when it doesn't resolve, e.g. while a ConfigMap volume swaps
// its links.
func linkTarget(path string) string {
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return ""
	}
	return resolved
}
