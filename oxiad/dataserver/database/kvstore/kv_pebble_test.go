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

package kvstore

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/oxia-db/oxia/oxiad/common/errs"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/cockroachdb/pebble/v2/vfs/errorfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/common/compare"
	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/failpoint"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/common/validation"
	"github.com/oxia-db/oxia/oxiad/common/crc"
)

func TestOxiaSlashSpanComparerMeetsPebbleContract(t *testing.T) {
	keys := [][]byte{
		[]byte("a"), []byte("b"), []byte("a/b"), []byte("a-c"), []byte("/a/b/c"), []byte("/b"),
		[]byte("/key/01499"), []byte("0"),
	}
	assert.NoError(t, pebble.CheckComparer(OxiaSlashSpanComparer.EnsureDefaults(), [][]byte{nil}, keys))
}

func TestPebbbleSimple(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_HIERARCHICAL)
	assert.NoError(t, err)

	wb := kv.NewWriteBatch()
	assert.NoError(t, wb.Put("a", []byte("0")))
	assert.NoError(t, wb.Put("b", []byte("1")))
	assert.NoError(t, wb.Put("c", []byte("2")))
	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	key, res, closer, err := kv.Get("a", ComparisonEqual, NoInternalKeys)
	assert.NoError(t, err)
	assert.Equal(t, "a", key)
	assert.Equal(t, "0", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("b", ComparisonEqual, NoInternalKeys)
	assert.NoError(t, err)
	assert.Equal(t, "b", key)
	assert.Equal(t, "1", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("c", ComparisonEqual, NoInternalKeys)
	assert.NoError(t, err)
	assert.Equal(t, "c", key)
	assert.Equal(t, "2", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("non-existing", ComparisonEqual, NoInternalKeys)
	assert.ErrorIs(t, err, errs.ErrDBKeyNotFound)
	assert.Equal(t, "", key)
	assert.Nil(t, res)
	assert.Nil(t, closer)

	wb = kv.NewWriteBatch()
	assert.NoError(t, wb.Put("a", []byte("00")))
	assert.NoError(t, wb.Put("b", []byte("11")))
	assert.NoError(t, wb.Put("d", []byte("22")))
	assert.NoError(t, wb.Delete("c"))

	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	key, res, closer, err = kv.Get("a", ComparisonEqual, NoInternalKeys)
	assert.NoError(t, err)
	assert.Equal(t, "a", key)
	assert.Equal(t, "00", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("b", ComparisonEqual, NoInternalKeys)
	assert.NoError(t, err)
	assert.Equal(t, "b", key)
	assert.Equal(t, "11", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("c", ComparisonEqual, NoInternalKeys)
	assert.ErrorIs(t, err, errs.ErrDBKeyNotFound)
	assert.Equal(t, "", key)
	assert.Nil(t, res)
	assert.Nil(t, closer)

	key, res, closer, err = kv.Get("d", ComparisonEqual, NoInternalKeys)
	assert.NoError(t, err)
	assert.Equal(t, "d", key)
	assert.Equal(t, "22", string(res))
	assert.NoError(t, closer.Close())

	assert.NoError(t, kv.Close())
	assert.NoError(t, factory.Close())
}

func TestPebbbleKeyRangeScan(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_HIERARCHICAL)
	assert.NoError(t, err)

	wb := kv.NewWriteBatch()
	assert.NoError(t, wb.Put("/root/a", []byte("a")))
	assert.NoError(t, wb.Put("/root/b", []byte("b")))
	assert.NoError(t, wb.Put("/root/c", []byte("c")))
	assert.NoError(t, wb.Put("/root/d", []byte("d")))
	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	it, err := kv.KeyRangeScan("/root/a", "/root/c", NoInternalKeys)
	assert.NoError(t, err)

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/a", it.Key())
	assert.True(t, it.Next())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/b", it.Key())
	assert.False(t, it.Next())

	assert.False(t, it.Valid())

	assert.NoError(t, it.Close())

	// Scan with empty result
	it, err = kv.KeyRangeScan("/xyz/a", "/xyz/c", NoInternalKeys)
	assert.NoError(t, err)
	assert.False(t, it.Valid())
	assert.NoError(t, it.Close())

	assert.NoError(t, kv.Close())
	assert.NoError(t, factory.Close())
}

func TestPebbbleKeyRangeScanReverse(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_HIERARCHICAL)
	assert.NoError(t, err)

	wb := kv.NewWriteBatch()
	assert.NoError(t, wb.Put("/root/a", []byte("a")))
	assert.NoError(t, wb.Put("/root/b", []byte("b")))
	assert.NoError(t, wb.Put("/root/c", []byte("c")))
	assert.NoError(t, wb.Put("/root/d", []byte("d")))
	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	it, err := kv.KeyRangeScanReverse("/root/a", "/root/c", NoInternalKeys)
	assert.NoError(t, err)

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/b", it.Key())
	assert.True(t, it.Prev())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/a", it.Key())
	assert.False(t, it.Prev())

	assert.False(t, it.Valid())

	assert.NoError(t, it.Close())

	// Scan with empty result
	it, err = kv.KeyRangeScanReverse("/xyz/a", "/xyz/c", NoInternalKeys)
	assert.NoError(t, err)
	assert.False(t, it.Valid())
	assert.NoError(t, it.Close())

	assert.NoError(t, kv.Close())
	assert.NoError(t, factory.Close())
}

func TestPebbleRangeScan(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_HIERARCHICAL)
	assert.NoError(t, err)

	wb := kv.NewWriteBatch()
	assert.NoError(t, wb.Put("/root/a", []byte("a")))
	assert.NoError(t, wb.Put("/root/b", []byte("b")))
	assert.NoError(t, wb.Put("/root/c", []byte("c")))
	assert.NoError(t, wb.Put("/root/d", []byte("d")))
	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	it, err := kv.RangeScan("/root/a", "/root/c", NoInternalKeys)
	assert.NoError(t, err)

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/a", it.Key())
	value, err := it.Value()
	assert.NoError(t, err)
	assert.Equal(t, "a", string(value))
	assert.True(t, it.Next())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/b", it.Key())
	value, err = it.Value()
	assert.NoError(t, err)
	assert.Equal(t, "b", string(value))
	assert.False(t, it.Next())

	assert.False(t, it.Valid())

	assert.NoError(t, it.Close())

	// Scan with empty result
	it, err = kv.RangeScan("/xyz/a", "/xyz/c", NoInternalKeys)
	assert.NoError(t, err)
	assert.False(t, it.Valid())
	assert.NoError(t, it.Close())

	assert.NoError(t, kv.Close())
	assert.NoError(t, factory.Close())
}

func TestPebbleRangeScanWithSlashOrder(t *testing.T) {
	keys := []string{
		"/a/a/a/zzzzzz",
		"/a/b/a/a/a/a",
		"/a/b/a/c",
		"/a/b/a/a",
		"/a/b/a/a/a",
		"/a/b/a/b",
	}

	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_HIERARCHICAL)
	assert.NoError(t, err)

	wb := kv.NewWriteBatch()

	for _, k := range keys {
		assert.NoError(t, wb.Put(k, []byte(k)))
	}

	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	it, err := kv.KeyRangeScan("/a/b/a/", "/a/b/a//", NoInternalKeys)
	assert.NoError(t, err)

	assert.True(t, it.Valid())
	assert.Equal(t, "/a/b/a/a", it.Key())
	assert.True(t, it.Next())

	assert.True(t, it.Valid())
	assert.Equal(t, "/a/b/a/b", it.Key())
	assert.True(t, it.Next())

	assert.True(t, it.Valid())
	assert.Equal(t, "/a/b/a/c", it.Key())
	assert.False(t, it.Next())

	assert.False(t, it.Valid())

	assert.NoError(t, it.Close())
}

// A range scan that includes its upper bound returns the key of the bound with
// the keys of the range, in either key sorting. Its end is the smallest stored
// key after the one of the bound: with the hierarchical sorting, it is not the
// one of the key followed by a zero byte for a key ending in "//", which has a
// level less than its separators. An empty upper bound is the empty key.
func TestPebbleRangeScanIncludeUpperBound(t *testing.T) {
	for _, keySorting := range []proto.KeySortingType{proto.KeySortingType_NATURAL, proto.KeySortingType_HIERARCHICAL} {
		t.Run(keySorting.String(), func(t *testing.T) {
			factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
			require.NoError(t, err)
			defer func() { assert.NoError(t, factory.Close()) }()
			kv, err := factory.NewKV(constant.DefaultNamespace, 1, keySorting)
			require.NoError(t, err)
			defer func() { assert.NoError(t, kv.Close()) }()
			p := kv.(*Pebble)

			keys := []string{"", "a", "a\x00", "a/", "a//", "a//\x00", "a//b", "a/b", "b", "/", "//", "/a//"}
			if keySorting == proto.KeySortingType_NATURAL {
				keys = append(keys, keyBeforeInternalRegion, keyAfterInternalRegion)
			}
			putAll(t, kv, keys...)
			putAll(t, kv, constant.InternalKeyPrefix+"a", constant.InternalKeyPrefix+"z")
			compareKeys := func(a, b string) int {
				return bytes.Compare(p.keyEncoder.Encode(a), p.keyEncoder.Encode(b))
			}

			for _, lower := range keys {
				for _, upper := range keys {
					var expected []string
					for _, key := range storedOrder(p, keys) {
						if compareKeys(lower, key) <= 0 && compareKeys(key, upper) <= 0 {
							expected = append(expected, key)
						}
					}
					it, err := kv.RangeScan(lower, upper, IteratorOpts{IncludeUpperBound: true})
					require.NoError(t, err)
					var scanned []string
					for ; it.Valid(); it.Next() {
						scanned = append(scanned, it.Key())
					}
					require.NoError(t, it.Close())
					assert.Equal(t, expected, scanned, "[%q, %q]", lower, upper)
				}
			}
		})
	}
}

func TestPebbleCompareKeys(t *testing.T) {
	keys := []string{"/", "a", "a.c", "a/", "a/y/z", "a0", "b/x", "__oxia/term", "__oxia/idx/i/a\x01a"}

	for _, sorting := range []proto.KeySortingType{proto.KeySortingType_NATURAL, proto.KeySortingType_HIERARCHICAL} {
		t.Run(sorting.String(), func(t *testing.T) {
			factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
			assert.NoError(t, err)
			kv, err := factory.NewKV(constant.DefaultNamespace, 1, sorting)
			assert.NoError(t, err)

			// The keys have to compare in the order the store returns them
			putAll(t, kv, keys...)
			sorted := scanAllKeys(t, kv, ShowInternalKeys)
			assert.Equal(t, len(keys), len(sorted))
			for i, a := range sorted {
				for j, b := range sorted {
					assert.Equal(t, cmp.Compare(i, j), kv.CompareKeys(a, b), "%q %q", a, b)
				}
			}

			assert.NoError(t, kv.Close())
			assert.NoError(t, factory.Close())
		})
	}
}

// The hierarchical sorting keeps the keys with a prefix in one range per level,
// with other keys between the ranges: here, the entries of the neighbouring
// indexes and other internal keys. However it is positioned and moved, the
// iterator has to return the keys with the prefix that sort before its end,
// and only them, in the order of the store.
func TestPebbleKeyPrefixIterator(t *testing.T) {
	keys := []string{"a", "b/c", "/a", "/a/b", "//",
		"__oxia/term", "__oxia/session/0001", "__oxia/session/0001/k", "__oxia/notifications/0001"}
	for _, index := range []string{"a", "b", "c"} {
		secondaryKeys := []string{"", "x", "/", "x/y", "x/y/z", "x/y/z/w", "x/y/z/w/v"}
		if index == "b" {
			// Levels where only the neighbouring indexes have entries
			secondaryKeys = []string{"", "x", "/", "x/y", "x/y/z/w/v"}
		}
		for _, secondaryKey := range secondaryKeys {
			keys = append(keys, "__oxia/idx/"+index+"/"+secondaryKey+"\x01p")
		}
	}
	probes := append([]string{"", "zzz", "__oxia/idx/b/", "__oxia/idx/b/x/y/y\x01", "__oxia/idx/b/x/y/z/w/v/u\x01"}, keys...)

	for _, sorting := range []proto.KeySortingType{proto.KeySortingType_NATURAL, proto.KeySortingType_HIERARCHICAL} {
		t.Run(sorting.String(), func(t *testing.T) {
			factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
			assert.NoError(t, err)
			kv, err := factory.NewKV(constant.DefaultNamespace, 1, sorting)
			assert.NoError(t, err)

			putAll(t, kv, keys...)
			all := scanAllKeys(t, kv, ShowInternalKeys)

			for _, prefix := range []string{"__oxia/idx/b/", "__oxia/idx/", "__oxia/idx/missing/", "/", "/a/"} {
				// The first probe, "", is no end
				for _, end := range probes {
					var expected []string
					for _, key := range all {
						if strings.HasPrefix(key, prefix) && (end == "" || kv.CompareKeys(key, end) < 0) {
							expected = append(expected, key)
						}
					}

					it, err := kv.KeyPrefixIterator(prefix, end)
					assert.NoError(t, err)

					var found []string
					for it.SeekGE(prefix); it.Valid(); it.Next() {
						found = append(found, it.Key())
					}
					assert.Equal(t, expected, found, "%q end %q", prefix, end)

					for _, probe := range probes {
						// The position of the first key with the prefix at or after the probe
						n := sort.Search(len(expected), func(i int) bool { return kv.CompareKeys(expected[i], probe) >= 0 })

						it.SeekGE(probe)
						assertIteratorAt(t, it, expected, n, "%q end %q SeekGE %q", prefix, end, probe)
						it.Prev()
						assertIteratorAt(t, it, expected, n-1, "%q end %q SeekGE %q, Prev", prefix, end, probe)

						it.SeekLT(probe)
						assertIteratorAt(t, it, expected, n-1, "%q end %q SeekLT %q", prefix, end, probe)
						it.Next()
						assertIteratorAt(t, it, expected, n, "%q end %q SeekLT %q, Next", prefix, end, probe)
					}

					assert.NoError(t, it.Close())
				}
			}

			assert.NoError(t, kv.Close())
			assert.NoError(t, factory.Close())
		})
	}
}

func assertIteratorAt(t *testing.T, it KeyIterator, keys []string, i int, msgAndArgs ...any) {
	t.Helper()
	if i < 0 || i >= len(keys) {
		assert.False(t, it.Valid(), msgAndArgs...)
		return
	}
	if assert.True(t, it.Valid(), msgAndArgs...) {
		assert.Equal(t, keys[i], it.Key(), msgAndArgs...)
	}
}

// Between two ranges of the keys with a prefix, the iterator must seek over
// the other keys, however many they are, and over the levels without keys.
func TestPebbleKeyPrefixIteratorSeeksOverOtherKeys(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_HIERARCHICAL)
	assert.NoError(t, err)

	// The prefix has a key at the levels 3 and 53. In between, 1000 keys sort
	// after it at level 3, and 1000 before it at level 53.
	deep := strings.Repeat("x/", 50)
	first, last := "__oxia/idx/b/k", "__oxia/idx/b/"+deep+"k"
	keys := []string{first, last}
	for i := range 1000 {
		keys = append(keys, fmt.Sprintf("__oxia/idx/c/%04d", i), fmt.Sprintf("__oxia/idx/a/%s%04d", deep, i))
	}
	putAll(t, kv, keys...)

	it, err := kv.KeyPrefixIterator("__oxia/idx/b/", "")
	assert.NoError(t, err)
	pi := it.(*PebblePrefixIterator).pi

	// One step onto the keys after the range at level 3, one seek that lands on
	// the keys before the range at level 53, and one seek over them
	assert.True(t, it.SeekGE(first))
	before := pi.Stats()
	assert.True(t, it.Next())
	assert.Equal(t, last, it.Key())
	after := pi.Stats()
	assert.Equal(t, 1, after.ForwardStepCount[pebble.InterfaceCall]-before.ForwardStepCount[pebble.InterfaceCall])
	assert.Equal(t, 2, after.ForwardSeekCount[pebble.InterfaceCall]-before.ForwardSeekCount[pebble.InterfaceCall])

	// And the same way back
	before = pi.Stats()
	assert.True(t, it.Prev())
	assert.Equal(t, first, it.Key())
	after = pi.Stats()
	assert.Equal(t, 1, after.ReverseStepCount[pebble.InterfaceCall]-before.ReverseStepCount[pebble.InterfaceCall])
	assert.Equal(t, 2, after.ReverseSeekCount[pebble.InterfaceCall]-before.ReverseSeekCount[pebble.InterfaceCall])

	assert.NoError(t, it.Close())
	assert.NoError(t, kv.Close())
	assert.NoError(t, factory.Close())
}

// A lookup reads any key of the store as it was when the iterator was created,
// in the flushed tables as in the memtable: the writes since don't show.
func TestPebblePrefixIteratorLookupReadsAtIteratorView(t *testing.T) {
	for _, sorting := range []proto.KeySortingType{proto.KeySortingType_NATURAL, proto.KeySortingType_HIERARCHICAL} {
		t.Run(sorting.String(), func(t *testing.T) {
			factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
			require.NoError(t, err)
			kv, err := factory.NewKV(constant.DefaultNamespace, 1, sorting)
			require.NoError(t, err)

			putAll(t, kv, "", "/a", "/a/b", "__oxia/idx/i/x\x01%2Fa", "zzz")
			require.NoError(t, kv.Flush())
			putAll(t, kv, "/c")

			it, err := kv.KeyPrefixIterator("__oxia/idx/i/", "")
			require.NoError(t, err)

			wb := kv.NewWriteBatch()
			require.NoError(t, wb.Put("/a", []byte("updated")))
			require.NoError(t, wb.Delete("/a/b"))
			require.NoError(t, wb.Delete("/c"))
			require.NoError(t, wb.Put("/new", []byte("new")))
			require.NoError(t, wb.Commit())
			require.NoError(t, wb.Close())

			for _, key := range []string{"", "/a", "/a/b", "/c", "zzz"} {
				value, err := it.Lookup(key)
				require.NoError(t, err, key)
				assert.Equal(t, "v-"+key, string(value), key)
			}
			for _, key := range []string{"/new", "/a/", "/b"} {
				_, err := it.Lookup(key)
				assert.ErrorIs(t, err, errs.ErrDBKeyNotFound, key)
			}

			// The lookups don't move the iterator
			assert.True(t, it.SeekGE("__oxia/idx/i/"))
			_, err = it.Lookup("/a")
			require.NoError(t, err)
			assert.Equal(t, "__oxia/idx/i/x\x01%2Fa", it.Key())
			assert.False(t, it.Next())

			assert.NoError(t, it.Close())
			assert.NoError(t, kv.Close())
			assert.NoError(t, factory.Close())
		})
	}
}

func TestPebbbleGetWithinBatch(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_HIERARCHICAL)
	assert.NoError(t, err)

	wb := kv.NewWriteBatch()
	assert.NoError(t, wb.Put("a", []byte("0")))
	assert.NoError(t, wb.Put("b", []byte("1")))
	assert.NoError(t, wb.Put("c", []byte("2")))

	value, closer, err := wb.Get("a")
	assert.NoError(t, err)
	assert.Equal(t, "0", string(value))
	assert.NoError(t, closer.Close())

	value, closer, err = wb.Get("non-existent")
	assert.ErrorIs(t, err, errs.ErrDBKeyNotFound)
	assert.Nil(t, value)
	assert.Nil(t, closer)

	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	// Second batch

	wb = kv.NewWriteBatch()
	value, closer, err = wb.Get("a")
	assert.NoError(t, err)
	assert.Equal(t, "0", string(value))
	assert.NoError(t, closer.Close())

	assert.NoError(t, wb.Put("a", []byte("00")))

	value, closer, err = wb.Get("a")
	assert.NoError(t, err)
	assert.Equal(t, "00", string(value))
	assert.NoError(t, closer.Close())

	assert.NoError(t, wb.Delete("a"))

	value, closer, err = wb.Get("a")
	assert.ErrorIs(t, err, errs.ErrDBKeyNotFound)
	assert.Nil(t, value)
	assert.Nil(t, closer)
	assert.NoError(t, wb.Close())

	assert.NoError(t, kv.Close())
	assert.NoError(t, factory.Close())
}

// From its second read on, a batch reads through one iterator, which must see
// the mutations made after it was created: a range deletion too, which pebble
// handles by rebuilding the iterator stack.
func TestPebbleBatchGetAfterDeleteRange(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
	require.NoError(t, err)

	wb := kv.NewWriteBatch()
	for _, key := range []string{"a", "b", "c"} {
		require.NoError(t, wb.Put(key, []byte(key)))
	}
	require.NoError(t, wb.Commit())
	require.NoError(t, wb.Close())

	wb = kv.NewWriteBatch()
	for _, key := range []string{"a", "b"} {
		value, closer, err := wb.Get(key)
		require.NoError(t, err)
		assert.Equal(t, key, string(value))
		require.NoError(t, closer.Close())
	}

	require.NoError(t, wb.DeleteRange("b", "c"))
	_, _, err = wb.Get("b")
	assert.ErrorIs(t, err, errs.ErrDBKeyNotFound)
	value, closer, err := wb.Get("c")
	require.NoError(t, err)
	assert.Equal(t, "c", string(value))
	require.NoError(t, closer.Close())

	require.NoError(t, wb.Put("b", []byte("bb")))
	value, closer, err = wb.Get("b")
	require.NoError(t, err)
	assert.Equal(t, "bb", string(value))
	require.NoError(t, closer.Close())
	require.NoError(t, wb.Close())

	require.NoError(t, kv.Close())
	require.NoError(t, factory.Close())
}

// PutMarshalable writes through pebble SetDeferred instead of Set: the entry
// must be readable back, byte-identical to a regular marshal, both within the
// same indexed batch (read-your-writes) and after the commit.
func TestPebblePutMarshalableWithinBatch(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_HIERARCHICAL)
	assert.NoError(t, err)

	sessionId := int64(42)
	entry := &proto.StorageEntry{
		Value:                 []byte("payload"),
		VersionId:             7,
		ModificationsCount:    3,
		CreationTimestamp:     100,
		ModificationTimestamp: 200,
		SessionId:             &sessionId,
	}
	expected, err := entry.MarshalVT()
	assert.NoError(t, err)

	wb := kv.NewWriteBatch()
	assert.NoError(t, wb.PutMarshalable("a", entry))

	// Read-your-writes within the same indexed batch
	value, closer, err := wb.Get("a")
	assert.NoError(t, err)
	assert.Equal(t, expected, value)
	assert.NoError(t, closer.Close())

	// Overwrite within the same batch through the same path
	entry.VersionId = 8
	expected2, err := entry.MarshalVT()
	assert.NoError(t, err)
	assert.NoError(t, wb.PutMarshalable("a", entry))

	value, closer, err = wb.Get("a")
	assert.NoError(t, err)
	assert.Equal(t, expected2, value)
	assert.NoError(t, closer.Close())

	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	// Visible after commit
	storedKey, value, closer, err := kv.Get("a", ComparisonEqual, NoInternalKeys)
	assert.NoError(t, err)
	assert.Equal(t, "a", storedKey)
	assert.Equal(t, expected2, value)
	assert.NoError(t, closer.Close())

	assert.NoError(t, kv.Close())
	assert.NoError(t, factory.Close())
}

func TestPebbbleDurability(t *testing.T) {
	options := NewFactoryOptionsForTest(t)

	// Open and write a key
	{
		factory, err := NewPebbleKVFactory(options)
		assert.NoError(t, err)
		kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
		assert.NoError(t, err)

		wb := kv.NewWriteBatch()
		assert.NoError(t, wb.Put("a", []byte("0")))
		assert.NoError(t, wb.Commit())
		assert.NoError(t, wb.Close())

		assert.NoError(t, kv.Close())
		assert.NoError(t, factory.Close())
	}

	// Open again and read it back
	{
		factory, err := NewPebbleKVFactory(options)
		assert.NoError(t, err)
		kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
		assert.NoError(t, err)

		key, res, closer, err := kv.Get("a", ComparisonEqual, NoInternalKeys)
		assert.NoError(t, err)
		assert.Equal(t, "a", key)
		assert.Equal(t, "0", string(res))
		assert.NoError(t, closer.Close())

		assert.NoError(t, kv.Close())
		assert.NoError(t, factory.Close())
	}
}

func TestPebbbleRangeScanInBatch(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_HIERARCHICAL)
	assert.NoError(t, err)

	wb := kv.NewWriteBatch()
	assert.NoError(t, wb.Put("/root/a", []byte("a")))
	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	wb = kv.NewWriteBatch()
	assert.NoError(t, wb.Put("/root/b", []byte("b")))
	assert.NoError(t, wb.Put("/root/c", []byte("c")))

	it, err := wb.KeyRangeScan("/root/a", "/root/c")
	assert.NoError(t, err)
	assert.True(t, it.Valid())
	assert.Equal(t, "/root/a", it.Key())
	assert.True(t, it.Next())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/b", it.Key())
	assert.False(t, it.Next())

	assert.False(t, it.Valid())

	assert.NoError(t, it.Close())

	assert.NoError(t, wb.Put("/root/d", []byte("d")))

	assert.NoError(t, wb.Delete("/root/a"))

	it, err = wb.KeyRangeScan("/root/a", "/root/c")
	assert.NoError(t, err)
	assert.True(t, it.Valid())
	assert.Equal(t, "/root/b", it.Key())
	assert.False(t, it.Next())

	assert.False(t, it.Valid())

	assert.NoError(t, it.Close())

	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	// Scan with empty result
	it, err = kv.KeyRangeScan("/xyz/a", "/xyz/c", NoInternalKeys)
	assert.NoError(t, err)
	assert.False(t, it.Valid())
	assert.NoError(t, it.Close())

	assert.NoError(t, kv.Close())
	assert.NoError(t, factory.Close())
}

// A batch scan of an inverted range is empty, and an upper bound encoded to an
// empty key leaves the range open, whatever the data pointer of the "".
func TestPebbleBatchRangeScanBounds(t *testing.T) {
	// The "" of a decoded request has no data pointer, while a "" literal may
	// have one, e.g. in a composite literal the compiler lays out statically
	emptyWithoutData := unsafe.String(nil, 0)
	require.Nil(t, unsafe.StringData(emptyWithoutData))
	emptyWithData := unsafe.String(new(byte), 0)
	require.NotNil(t, unsafe.StringData(emptyWithData))

	for _, test := range []struct {
		name         string
		keySorting   proto.KeySortingType
		lower, upper string
		expected     []string
	}{
		{"natural inverted", proto.KeySortingType_NATURAL, "c", "a", nil},
		{"hierarchical inverted", proto.KeySortingType_HIERARCHICAL, "c", "a", nil},
		// "/a/b" has more separators, so it sorts after "/b"
		{"hierarchical inverted levels", proto.KeySortingType_HIERARCHICAL, "/a/b", "/b", nil},
		{"natural empty end", proto.KeySortingType_NATURAL, "b", emptyWithoutData, []string{"b", "c"}},
		{"natural empty end with data", proto.KeySortingType_NATURAL, "b", emptyWithData, []string{"b", "c"}},
		// The empty key sorts first with the hierarchical encoder
		{"hierarchical empty end", proto.KeySortingType_HIERARCHICAL, "b", emptyWithoutData, nil},
		{"hierarchical empty end with data", proto.KeySortingType_HIERARCHICAL, "b", emptyWithData, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
			require.NoError(t, err)
			kv, err := factory.NewKV(constant.DefaultNamespace, 1, test.keySorting)
			require.NoError(t, err)

			wb := kv.NewWriteBatch()
			for _, key := range []string{"a", "b", "c", "/a/b", "/b"} {
				require.NoError(t, wb.Put(key, []byte(key)))
			}

			it, err := wb.KeyRangeScan(test.lower, test.upper)
			require.NoError(t, err)
			assert.Equal(t, test.expected, toList(it))

			assert.NoError(t, wb.Close())
			assert.NoError(t, kv.Close())
			assert.NoError(t, factory.Close())
		})
	}
}

func TestPebbbleDeleteRangeInBatch(t *testing.T) {
	keys := []string{
		"/a/a/a/zzzzzz",
		"/a/b/a/a/a/a",
		"/a/b/a/c",
		"/a/b/a/a",
		"/a/b/a/a/a",
		"/a/b/a/b",
	}

	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_HIERARCHICAL)
	assert.NoError(t, err)

	wb := kv.NewWriteBatch()

	for _, k := range keys {
		assert.NoError(t, wb.Put(k, []byte(k)))
	}

	err = wb.DeleteRange("/a/b/a/", "/a/b/a//")
	assert.NoError(t, err)

	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	key, res, closer, err := kv.Get("/a/b/a/a", ComparisonEqual, NoInternalKeys)
	assert.Nil(t, res)
	assert.Nil(t, closer)
	assert.Equal(t, "", key)
	assert.ErrorIs(t, err, errs.ErrDBKeyNotFound)

	key, res, closer, err = kv.Get("/a/a/a/zzzzzz", ComparisonEqual, NoInternalKeys)
	assert.Nil(t, err)
	assert.Equal(t, "/a/a/a/zzzzzz", key)
	assert.Equal(t, "/a/a/a/zzzzzz", string(res))
	assert.NoError(t, closer.Close())

	assert.NoError(t, kv.Close())
	assert.NoError(t, factory.Close())
}

func TestPebbbleDoubleOpen(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
	assert.NoError(t, err)

	kv2, err2 := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
	assert.Error(t, err2)
	assert.Nil(t, kv2)

	assert.NoError(t, kv.Close())
}

func TestPebbleCloseClosesDbWhenFlushFails(t *testing.T) {
	factory, kv := newKVWithFailpoint(t, "close.flush")

	assert.ErrorIs(t, kv.Close(), errFailpoint)
	assert.Len(t, factory.memTableSlots, maxMemTableSlots)

	reopened, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
	require.NoError(t, err)
	assert.ErrorIs(t, reopened.Close(), errFailpoint)
}

func TestPebbleSnapshot(t *testing.T) {
	copiedLocation := t.TempDir()
	copiedLocationDbPath := filepath.Join(copiedLocation, constant.DefaultNamespace, "shard-1")

	{
		factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
		assert.NoError(t, err)
		kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
		assert.NoError(t, err)

		for i := 0; i < 100; i++ {
			wb := kv.NewWriteBatch()
			for j := 0; j < 100; j++ {
				assert.NoError(t, wb.Put(fmt.Sprintf("key-%d-%d", i, j),
					[]byte(fmt.Sprintf("value-%d-%d", i, j))))
			}

			assert.NoError(t, wb.Commit())
			assert.NoError(t, wb.Close())
		}

		s, err := kv.Snapshot()
		assert.NoError(t, err)

		// Copy the snapshot to a new directory
		assert.NoError(t, os.MkdirAll(copiedLocationDbPath, 0755))

		for ; s.Valid(); s.Next() {
			f, err := s.Chunk()
			assert.NoError(t, err)
			content := f.Content()
			fileName := f.Name()
			file, err := os.OpenFile(filepath.Join(copiedLocationDbPath, fileName), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
			assert.NoError(t, err)
			for len(content) > 0 {
				n, err := file.Write(content)
				assert.NoError(t, err)
				content = content[n:]
			}
			assert.NoError(t, file.Close())
		}

		_, err = os.Stat(s.BasePath())
		assert.NoError(t, err)

		// Closing the snapshot must get rid of its directory
		assert.NoError(t, s.Close())

		_, err = os.Stat(s.BasePath())
		assert.ErrorIs(t, err, os.ErrNotExist)

		assert.NoError(t, kv.Close())
	}

	{
		// Open the database from the copied location
		factory2, err := NewPebbleKVFactory(&FactoryOptions{
			DataDir:     copiedLocation,
			CacheSizeMB: 1,
		})
		assert.NoError(t, err)
		kv2, err := factory2.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
		assert.NoError(t, err)

		for i := 0; i < 100; i++ {
			for j := 0; j < 100; j++ {
				k := fmt.Sprintf("key-%d-%d", i, j)
				key, r, closer, err := kv2.Get(k, ComparisonEqual, NoInternalKeys)
				assert.NoError(t, err)
				assert.Equal(t, k, key)
				assert.Equal(t, fmt.Sprintf("value-%d-%d", i, j), string(r))
				assert.NoError(t, closer.Close())
			}
		}

		assert.NoError(t, kv2.Close())
		assert.NoError(t, factory2.Close())
	}
}

// A snapshot a caller keeps open outlives its database when the shard is
// closed and reopened, e.g. a follower promoted to leader: the reopened
// database must not name its next snapshot after the one still held.
func TestPebbleSnapshotPathsStayUniqueAcrossReopen(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
	require.NoError(t, err)
	held, err := kv.Snapshot()
	require.NoError(t, err)
	require.NoError(t, kv.Close())

	kv, err = factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
	require.NoError(t, err)
	next, err := kv.Snapshot()
	require.NoError(t, err)
	assert.NotEqual(t, held.BasePath(), next.BasePath())
	_, err = os.Stat(held.BasePath())
	assert.NoError(t, err, "the held snapshot is intact")

	assert.NoError(t, next.Close())
	assert.NoError(t, held.Close())
	assert.NoError(t, kv.Close())
	assert.NoError(t, factory.Close())
}

func TestPebbleDatabaseGenerationCountsReplacementsAndDeletions(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
	require.NoError(t, err)
	assert.EqualValues(t, 0, factory.DatabaseGeneration(constant.DefaultNamespace, 1))
	require.NoError(t, kv.Close())

	loader, err := factory.NewSnapshotLoader(context.Background(), constant.DefaultNamespace, 1, SnapshotLoaderOptions{})
	require.NoError(t, err)
	require.NoError(t, loader.Close())
	assert.EqualValues(t, 0, factory.DatabaseGeneration(constant.DefaultNamespace, 1),
		"a loader that doesn't install keeps the database")

	installSnapshot(t, factory, snapshotChunksOf(t, "new"))
	assert.EqualValues(t, 1, factory.DatabaseGeneration(constant.DefaultNamespace, 1), "an install replaces the database")
	assert.EqualValues(t, 0, factory.DatabaseGeneration(constant.DefaultNamespace, 2), "other shards are not affected")

	kv, err = factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
	require.NoError(t, err)
	assert.EqualValues(t, 1, factory.DatabaseGeneration(constant.DefaultNamespace, 1), "reopening keeps the database")
	require.NoError(t, kv.Delete())
	assert.EqualValues(t, 2, factory.DatabaseGeneration(constant.DefaultNamespace, 1))
	assert.NoError(t, factory.Close())
}

func TestPebbleSnapshotLoaderInstallReplacesTheDatabase(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	defer func() { assert.NoError(t, factory.Close()) }()
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
	require.NoError(t, err)
	putKeys(t, kv, "old")
	require.NoError(t, kv.Close())

	loader, err := factory.NewSnapshotLoader(context.Background(), constant.DefaultNamespace, 1, SnapshotLoaderOptions{})
	require.NoError(t, err)
	addChunks(t, loader, snapshotChunksOf(t, "new"))
	var checked []string
	logClears := 0
	require.NoError(t, loader.Install(func(dir string) error {
		checked = append(checked, dir)
		return nil
	}, nil, func() error {
		logClears++
		return nil
	}))
	require.NoError(t, loader.Close())

	paths := newInstallPaths(factory.(*PebbleFactory).getKVPath(constant.DefaultNamespace, 1))
	assert.Equal(t, []string{paths.stage}, checked, "the check reads the snapshot before it replaces the database")
	assert.Equal(t, 1, logClears)
	assertNoInstallFiles(t, vfs.Default, paths)
	kv, err = factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
	require.NoError(t, err)
	assertKeys(t, kv, "new")
	assertNoKeys(t, kv, "old")
	assert.NoError(t, kv.Close())
}

func TestPebbleSnapshotLoaderInstallKeepsTheDatabaseWhenTheCheckFails(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	defer func() { assert.NoError(t, factory.Close()) }()
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
	require.NoError(t, err)
	putKeys(t, kv, "old")
	require.NoError(t, kv.Close())

	loader, err := factory.NewSnapshotLoader(context.Background(), constant.DefaultNamespace, 1, SnapshotLoaderOptions{})
	require.NoError(t, err)
	addChunks(t, loader, snapshotChunksOf(t, "new"))
	errCheck := errors.New("the snapshot is not the announced one")
	logClears := 0
	err = loader.Install(func(string) error { return errCheck }, nil, func() error {
		logClears++
		return nil
	})
	assert.ErrorIs(t, err, errCheck)
	require.NoError(t, loader.Close())

	assert.Zero(t, logClears)
	assert.EqualValues(t, 0, factory.DatabaseGeneration(constant.DefaultNamespace, 1))
	assertNoInstallFiles(t, vfs.Default, newInstallPaths(factory.(*PebbleFactory).getKVPath(constant.DefaultNamespace, 1)))
	kv, err = factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
	require.NoError(t, err)
	assertKeys(t, kv, "old")
	assert.NoError(t, kv.Close())
}

// What prepare writes to the snapshot, once checked, is in the database the
// snapshot replaces, whether prepare closes the snapshot or leaves it to the
// install.
func TestPebbleSnapshotLoaderInstallPreparesTheSnapshotBeforeItReplacesTheDatabase(t *testing.T) {
	for _, closes := range []bool{false, true} {
		t.Run(fmt.Sprintf("closes=%v", closes), func(t *testing.T) {
			factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
			require.NoError(t, err)
			defer func() { assert.NoError(t, factory.Close()) }()
			kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
			require.NoError(t, err)
			putKeys(t, kv, "old")
			require.NoError(t, kv.Close())

			loader, err := factory.NewSnapshotLoader(context.Background(), constant.DefaultNamespace, 1,
				SnapshotLoaderOptions{})
			require.NoError(t, err)
			addChunks(t, loader, snapshotChunksOf(t, "new"))
			var steps []string
			require.NoError(t, loader.Install(func(string) error {
				steps = append(steps, "check")
				return nil
			}, func(stage KV) error {
				steps = append(steps, "prepare")
				assertKeys(t, stage, "new")
				putKeys(t, stage, "prepared")
				if closes {
					return stage.Close()
				}
				return nil
			}, noLogClear))
			require.NoError(t, loader.Close())

			assert.Equal(t, []string{"check", "prepare"}, steps)
			assertNoInstallFiles(t, vfs.Default, newInstallPaths(factory.(*PebbleFactory).getKVPath(constant.DefaultNamespace, 1)))
			kv, err = factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
			require.NoError(t, err)
			assertKeys(t, kv, "new")
			assertKeys(t, kv, "prepared")
			assertNoKeys(t, kv, "old")
			assert.NoError(t, kv.Close())
		})
	}
}

// A snapshot that prepare fails to write to, or that doesn't open as a
// database, doesn't replace the database.
func TestPebbleSnapshotLoaderInstallKeepsTheDatabaseWhenThePreparationFails(t *testing.T) {
	errPrepare := errors.New("the snapshot can't be prepared")
	unknownEncoding := func(chunks []snapshotChunk) []snapshotChunk {
		for i, chunk := range chunks {
			if chunk.name == markerFileName {
				chunks[i].content = []byte("unknown-format")
			}
		}
		return chunks
	}
	for _, test := range []struct {
		name    string
		chunks  func([]snapshotChunk) []snapshotChunk
		prepare func(stage KV) error
		err     string
	}{
		{"prepare", func(chunks []snapshotChunk) []snapshotChunk { return chunks },
			func(KV) error { return errPrepare }, errPrepare.Error()},
		{"open", unknownEncoding, func(KV) error { return nil }, "failed to open the snapshot"},
	} {
		t.Run(test.name, func(t *testing.T) {
			factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
			require.NoError(t, err)
			defer func() { assert.NoError(t, factory.Close()) }()
			kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
			require.NoError(t, err)
			putKeys(t, kv, "old")
			require.NoError(t, kv.Close())

			loader, err := factory.NewSnapshotLoader(context.Background(), constant.DefaultNamespace, 1,
				SnapshotLoaderOptions{})
			require.NoError(t, err)
			addChunks(t, loader, test.chunks(snapshotChunksOf(t, "new")))
			logClears := 0
			err = loader.Install(noCheck, test.prepare, func() error {
				logClears++
				return nil
			})
			assert.ErrorContains(t, err, "failed to prepare stage")
			assert.ErrorContains(t, err, test.err)
			require.NoError(t, loader.Close())

			assert.Zero(t, logClears)
			assert.EqualValues(t, 0, factory.DatabaseGeneration(constant.DefaultNamespace, 1))
			assertNoInstallFiles(t, vfs.Default, newInstallPaths(factory.(*PebbleFactory).getKVPath(constant.DefaultNamespace, 1)))
			kv, err = factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
			require.NoError(t, err)
			assertKeys(t, kv, "old")
			assert.NoError(t, kv.Close())
		})
	}
}

func TestPebbleSnapshotLoaderCloseKeepsTheDatabase(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	defer func() { assert.NoError(t, factory.Close()) }()
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
	require.NoError(t, err)
	putKeys(t, kv, "old")
	require.NoError(t, kv.Close())

	loader, err := factory.NewSnapshotLoader(context.Background(), constant.DefaultNamespace, 1, SnapshotLoaderOptions{})
	require.NoError(t, err)
	chunks := snapshotChunksOf(t, "new")
	// The stream ends in the middle of the last file
	last := chunks[len(chunks)-1]
	addChunks(t, loader, chunks[:len(chunks)-1])
	require.NoError(t, loader.AddChunk(last.name, 0, 2, bytes.Clone(last.content)))
	require.NoError(t, loader.Close())

	assertNoInstallFiles(t, vfs.Default, newInstallPaths(factory.(*PebbleFactory).getKVPath(constant.DefaultNamespace, 1)))
	kv, err = factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
	require.NoError(t, err)
	assertKeys(t, kv, "old")
	assert.NoError(t, kv.Close())
}

func TestPebbleSnapshotLoaderInstallRefusesAnUnfinishedFile(t *testing.T) {
	factory := newMemFactory(t, memFSWithRoom(), nil)
	loader, err := factory.NewSnapshotLoader(context.Background(), constant.DefaultNamespace, 1, SnapshotLoaderOptions{})
	require.NoError(t, err)
	require.NoError(t, loader.AddChunk("a", 0, 2, []byte("first")))

	err = loader.Install(noCheck, nil, noLogClear)
	assert.ErrorContains(t, err, "last file not finished")
	assert.NoError(t, loader.Close())
}

func TestPebbleSnapshotLoaderInstallLeavesTheReplacementToRecoveryWhenTheLogClearFails(t *testing.T) {
	fs := memFSWithRoom()
	factory := newMemFactory(t, fs, nil)
	dbPath := factory.getKVPath(constant.DefaultNamespace, 1)
	writeMemDB(t, fs, dbPath, "old")

	loader, err := factory.NewSnapshotLoader(context.Background(), constant.DefaultNamespace, 1, SnapshotLoaderOptions{})
	require.NoError(t, err)
	addChunks(t, loader, snapshotChunksOf(t, "new"))
	errClear := errors.New("the log can't be cleared")
	assert.ErrorIs(t, loader.Install(noCheck, nil, func() error { return errClear }), errClear)
	require.NoError(t, loader.Close())

	logClears := 0
	require.NoError(t, factory.RecoverSnapshotInstall(constant.DefaultNamespace, 1, func() error {
		logClears++
		return nil
	}))
	assert.Equal(t, 1, logClears)
	assert.EqualValues(t, 1, factory.DatabaseGeneration(constant.DefaultNamespace, 1),
		"recovery raises the generation to the one of the install, once")
	assert.Equal(t, map[string]int{"new": 100}, keysOnFS(t, fs, dbPath))
	assertNoInstallFiles(t, fs, newInstallPaths(dbPath))
}

// The leader never sends the snapshot again once the follower acked it, so the
// install must leave the shard with its database or the snapshot after a
// machine crash at any step, which loses whatever was not synced to disk.
func TestPebbleSnapshotLoaderInstallSurvivesACrashAtEveryStep(t *testing.T) {
	skipWithoutFailpoints(t)
	chunks := snapshotChunksOf(t, "new")
	for _, test := range []struct {
		step string
		// replaced is whether the snapshot is the database after the crash
		replaced bool
	}{
		{"check-marker", false},
		{"remove-stage", false},
		{"create-stage", false},
		{"check-room", false},
		{"check-files", false},
		{"check-stage", false},
		{"prepare-stage", false},
		{"sync-stage", false},
		{"mark", false},
		{"swap", true},
		{"clear-log", true},
		{"remove-replaced", true},
		{"unmark", true},
	} {
		t.Run(test.step, func(t *testing.T) {
			fs := memFSWithRoom()
			var crashed *vfs.MemFS
			factory := newMemFactory(t, fs, failpoint.New(map[string]func() error{
				"snapshot.install." + test.step: func() error {
					crashed = fs.CrashClone(vfs.CrashCloneCfg{})
					return errFailpoint
				},
			}))
			dbPath := factory.getKVPath(constant.DefaultNamespace, 1)
			writeMemDB(t, fs, dbPath, "old")

			loader, err := factory.NewSnapshotLoader(context.Background(), constant.DefaultNamespace, 1, SnapshotLoaderOptions{})
			if err == nil {
				addChunks(t, loader, chunks)
				err = loader.Install(noCheck, nil, noLogClear)
				require.NoError(t, loader.Close())
			}
			require.ErrorIs(t, err, errFailpoint)
			require.NotNil(t, crashed)

			// The server restarts on what the crash kept
			restarted := newMemFactory(t, crashed, nil)
			logClears := 0
			require.NoError(t, restarted.RecoverSnapshotInstall(constant.DefaultNamespace, 1, func() error {
				logClears++
				return nil
			}))
			if test.replaced {
				assert.Equal(t, map[string]int{"new": 100}, keysOnFS(t, crashed, dbPath))
				assert.Equal(t, 1, logClears, "the entries of the log belong to the replaced database")
			} else {
				assert.Equal(t, map[string]int{"old": 100}, keysOnFS(t, crashed, dbPath))
				assert.Zero(t, logClears)
			}
			assertNoInstallFiles(t, crashed, newInstallPaths(dbPath))
		})
	}
}

func TestPebbleRecoverSnapshotInstallCompletesAnInterruptedSwap(t *testing.T) {
	for _, test := range []struct {
		name      string
		interrupt func(t *testing.T, fs vfs.FS, paths installPaths)
	}{
		{"database-moved-aside", func(t *testing.T, fs vfs.FS, paths installPaths) {
			t.Helper()
			require.NoError(t, fs.Rename(paths.db, paths.replaced))
		}},
		{"replaced-database-left-over", func(t *testing.T, fs vfs.FS, paths installPaths) {
			t.Helper()
			writeMemDB(t, fs, paths.replaced, "older")
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fs := memFSWithRoom()
			factory := newMemFactory(t, fs, nil)
			paths := newInstallPaths(factory.getKVPath(constant.DefaultNamespace, 1))
			writeMemDB(t, fs, paths.db, "old")
			writeMemDB(t, fs, paths.stage, "new")
			require.NoError(t, writeInstallMarker(fs, paths.marker, 3))
			test.interrupt(t, fs, paths)

			require.NoError(t, factory.RecoverSnapshotInstall(constant.DefaultNamespace, 1, noLogClear))

			assert.Equal(t, map[string]int{"new": 100}, keysOnFS(t, fs, paths.db))
			assert.EqualValues(t, 3, factory.DatabaseGeneration(constant.DefaultNamespace, 1))
			assertNoInstallFiles(t, fs, paths)
		})
	}
}

func TestPebbleRecoverSnapshotInstallTakesTheNextGenerationFromAMarkerWithoutContent(t *testing.T) {
	fs := memFSWithRoom()
	factory := newMemFactory(t, fs, nil)
	paths := newInstallPaths(factory.getKVPath(constant.DefaultNamespace, 1))
	writeMemDB(t, fs, paths.db, "old")
	writeMemDB(t, fs, paths.stage, "new")
	marker, err := fs.Create(paths.marker, vfs.WriteCategoryUnspecified)
	require.NoError(t, err)
	require.NoError(t, marker.Close())

	require.NoError(t, factory.RecoverSnapshotInstall(constant.DefaultNamespace, 1, noLogClear))

	assert.Equal(t, map[string]int{"new": 100}, keysOnFS(t, fs, paths.db))
	assert.EqualValues(t, 1, factory.DatabaseGeneration(constant.DefaultNamespace, 1))
	assertNoInstallFiles(t, fs, paths)
}

func TestPebbleRecoverSnapshotInstallRejectsAnInvalidNamespace(t *testing.T) {
	factory := newMemFactory(t, memFSWithRoom(), nil)
	err := factory.RecoverSnapshotInstall("../escaped-ns", 1, noLogClear)
	assert.ErrorContains(t, err, "invalid path traversal sequence")
}

func TestPebbleNewSnapshotLoaderChecksTheRoomOnTheDisk(t *testing.T) {
	const size = 1 << 30
	for _, test := range []struct {
		name     string
		total    uint64
		avail    uint64
		admitted bool
	}{
		// The headroom is 5% of the disk, at least 1 GiB and at most 8 GiB
		{"room-for-the-snapshot-and-5%", 100 << 30, size + 5<<30, true},
		{"short-of-5%", 100 << 30, size + 5<<30 - 1, false},
		{"room-for-the-snapshot-and-1-gib", 10 << 30, size + 1<<30, true},
		{"short-of-1-gib", 10 << 30, size + 1<<30 - 1, false},
		{"room-for-the-snapshot-and-8-gib", 500 << 30, size + 8<<30, true},
		{"short-of-8-gib", 500 << 30, size + 8<<30 - 1, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			fs := vfs.NewMem()
			fs.TestingSetDiskUsage(vfs.DiskUsage{TotalBytes: test.total, AvailBytes: test.avail})
			factory := newMemFactory(t, fs, nil)

			loader, err := factory.NewSnapshotLoader(context.Background(), constant.DefaultNamespace, 1, SnapshotLoaderOptions{Size: size})
			if test.admitted {
				require.NoError(t, err)
				assert.NoError(t, loader.Close())
				return
			}
			assert.ErrorIs(t, err, errs.ErrUnavailable)
			assert.ErrorContains(t, err, fmt.Sprintf("the disk has %d free", test.avail))
			assertNoInstallFiles(t, fs, newInstallPaths(factory.getKVPath(constant.DefaultNamespace, 1)))

			// The refusal frees the install slot
			loader, err = factory.NewSnapshotLoader(context.Background(), constant.DefaultNamespace, 1, SnapshotLoaderOptions{})
			require.NoError(t, err)
			assert.NoError(t, loader.Close())
		})
	}
}

// The installs of one group run side by side, the first with the room check
// for the group, while any other waits for the last of them to end, as does
// another install of a shard of the group.
func TestPebbleNewSnapshotLoaderAdmitsTheInstallsOfAGroupSideBySide(t *testing.T) {
	factory := newMemFactory(t, memFSWithRoom(), nil)
	first, err := factory.NewSnapshotLoader(context.Background(), constant.DefaultNamespace, 1,
		SnapshotLoaderOptions{Group: "split"})
	require.NoError(t, err)
	// No disk holds this size: the second of the group joins without a check
	second, err := factory.NewSnapshotLoader(context.Background(), constant.DefaultNamespace, 2,
		SnapshotLoaderOptions{Group: "split", Size: math.MaxInt64 / 2})
	require.NoError(t, err)

	waits := func() {
		t.Helper()
		for _, group := range []string{"", "other"} {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err := factory.NewSnapshotLoader(ctx, constant.DefaultNamespace, 3, SnapshotLoaderOptions{Group: group})
			assert.ErrorIs(t, err, context.Canceled, "group %q", group)
		}
	}
	waits()
	// Two installs of one shard share its stage
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = factory.NewSnapshotLoader(ctx, constant.DefaultNamespace, 2, SnapshotLoaderOptions{Group: "split"})
	assert.ErrorIs(t, err, context.Canceled, "a second install of a shard of the group")
	require.NoError(t, first.Close())
	waits()
	require.NoError(t, second.Close())

	third, err := factory.NewSnapshotLoader(context.Background(), constant.DefaultNamespace, 3, SnapshotLoaderOptions{})
	require.NoError(t, err)
	assert.NoError(t, third.Close())
}

// The install of a snapshot that got as far as its marker completes before
// another starts: the next one would remove its stage, and the marker would
// adopt the next one's.
func TestPebbleNewSnapshotLoaderRefusesWhileAnInstallIsMarked(t *testing.T) {
	fs := memFSWithRoom()
	factory := newMemFactory(t, fs, nil)
	paths := newInstallPaths(factory.getKVPath(constant.DefaultNamespace, 1))
	writeMemDB(t, fs, paths.stage, "new")
	require.NoError(t, writeInstallMarker(fs, paths.marker, 1))

	_, err := factory.NewSnapshotLoader(context.Background(), constant.DefaultNamespace, 1, SnapshotLoaderOptions{})
	assert.ErrorIs(t, err, errs.ErrFailedPrecondition)
	assert.Equal(t, map[string]int{"new": 100}, keysOnFS(t, fs, paths.stage), "the marked snapshot stands")

	require.NoError(t, factory.RecoverSnapshotInstall(constant.DefaultNamespace, 1, noLogClear))
	loader, err := factory.NewSnapshotLoader(context.Background(), constant.DefaultNamespace, 1, SnapshotLoaderOptions{})
	require.NoError(t, err, "the slot is free again")
	assert.NoError(t, loader.Close())
}

// The installs of a group join once the room check of its first passed. A
// refused check ends the run of the group: each install that waited is then
// checked on its own.
func TestPebbleNewSnapshotLoaderJoinsAGroupOnceItsRoomIsChecked(t *testing.T) {
	skipWithoutFailpoints(t)
	inCheck := make(chan struct{})
	unblock := make(chan struct{})
	var blocked atomic.Bool
	fs := memFSWithRoom()
	factory := newMemFactory(t, fs, failpoint.New(map[string]func() error{
		"snapshot.install.check-room": func() error {
			if blocked.CompareAndSwap(false, true) {
				close(inCheck)
				<-unblock
			}
			return nil
		},
	}))
	huge := SnapshotLoaderOptions{Group: "split", Size: math.MaxInt64 / 4}

	firstErr := make(chan error, 1)
	go func() {
		_, err := factory.NewSnapshotLoader(context.Background(), constant.DefaultNamespace, 1, huge)
		firstErr <- err
	}()
	<-inCheck
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := factory.NewSnapshotLoader(ctx, constant.DefaultNamespace, 2, huge)
	assert.ErrorIs(t, err, context.Canceled, "a member waits for the room check of the first")

	close(unblock)
	assert.ErrorIs(t, <-firstErr, errs.ErrUnavailable)
	for _, shard := range []int64{2, 1} {
		_, err := factory.NewSnapshotLoader(context.Background(), constant.DefaultNamespace, shard, huge)
		assert.ErrorIs(t, err, errs.ErrUnavailable, "shard %d is checked on its own", shard)
	}
}

// A shard joins a run of its group once: an install that waits for the group
// gets in once its members are done, however they come and go.
func TestPebbleNewSnapshotLoaderJoinsARunOfItsGroupOnce(t *testing.T) {
	factory := newMemFactory(t, memFSWithRoom(), nil)
	group := SnapshotLoaderOptions{Group: "split"}
	first, err := factory.NewSnapshotLoader(context.Background(), constant.DefaultNamespace, 1, group)
	require.NoError(t, err)
	second, err := factory.NewSnapshotLoader(context.Background(), constant.DefaultNamespace, 2, group)
	require.NoError(t, err)

	lone := make(chan error, 1)
	go func() {
		loader, err := factory.NewSnapshotLoader(context.Background(), constant.DefaultNamespace, 3, SnapshotLoaderOptions{})
		if err == nil {
			err = loader.Close()
		}
		lone <- err
	}()

	require.NoError(t, first.Close())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = factory.NewSnapshotLoader(ctx, constant.DefaultNamespace, 1, group)
	assert.ErrorIs(t, err, context.Canceled, "a shard that left the run waits for it to end")

	require.NoError(t, second.Close())
	select {
	case err := <-lone:
		assert.NoError(t, err)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "the lone install waited past the run of the group")
	}
}

func TestPebbleNewSnapshotLoaderWaitsForTheInstallInProgress(t *testing.T) {
	factory := newMemFactory(t, memFSWithRoom(), nil)
	first, err := factory.NewSnapshotLoader(context.Background(), constant.DefaultNamespace, 1, SnapshotLoaderOptions{})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = factory.NewSnapshotLoader(ctx, constant.DefaultNamespace, 2, SnapshotLoaderOptions{})
	assert.ErrorIs(t, err, context.Canceled)

	admitted := make(chan error, 1)
	go func() {
		second, err := factory.NewSnapshotLoader(context.Background(), constant.DefaultNamespace, 2, SnapshotLoaderOptions{})
		if err == nil {
			err = second.Close()
		}
		admitted <- err
	}()
	require.NoError(t, first.Close())
	assert.NoError(t, <-admitted)
}

func TestPebbleDeleteRemovesTheFilesOfAnUnfinishedInstall(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	defer func() { assert.NoError(t, factory.Close()) }()
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
	require.NoError(t, err)
	paths := newInstallPaths(factory.(*PebbleFactory).getKVPath(constant.DefaultNamespace, 1))
	require.NoError(t, os.MkdirAll(paths.stage, 0755))
	require.NoError(t, os.MkdirAll(paths.replaced, 0755))
	require.NoError(t, os.WriteFile(paths.marker, []byte("1"), 0600))

	require.NoError(t, kv.Delete())

	assertNoInstallFiles(t, vfs.Default, paths)
	_, err = os.Stat(paths.db)
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestPebbleSnapshotLoader_RejectsPathTraversal(t *testing.T) {
	dataDir := t.TempDir()
	factory, err := NewPebbleKVFactory(&FactoryOptions{
		DataDir:     dataDir,
		CacheSizeMB: 1,
	})
	require.NoError(t, err)

	loader, err := factory.NewSnapshotLoader(context.Background(), constant.DefaultNamespace, 1, SnapshotLoaderOptions{})
	require.NoError(t, err)

	stagePath := filepath.Join(dataDir, constant.DefaultNamespace, "shard-1.staging")
	// Point the chunk name above the data dir; the sender only ever emits plain
	// file names, so a name carrying ".." can only come from a hostile peer.
	outside := filepath.Join(filepath.Dir(dataDir), "escape-marker.txt")
	rel, err := filepath.Rel(stagePath, outside)
	require.NoError(t, err)

	err = loader.AddChunk(rel, 0, 1, []byte("payload"))
	assert.Error(t, err)

	_, statErr := os.Stat(outside)
	assert.True(t, os.IsNotExist(statErr))

	// Anything else that isn't a plain file name is rejected too, so a peer can't
	// reach into a nested path or name the loader dir itself.
	for _, fileName := range []string{"", ".", "..", "sub/dir/file", "./MANIFEST-000001", outside} {
		assert.Error(t, loader.AddChunk(fileName, 0, 1, []byte("payload")), "expected %q to be rejected", fileName)
	}

	// A name the sender actually emits still goes through.
	assert.NoError(t, loader.AddChunk(markerFileName, 0, 1, []byte("payload")))

	assert.NoError(t, loader.Close())
	assert.NoError(t, factory.Close())
}

func TestPebbleFactoryNewKVRejectsEmptyNamespace(t *testing.T) {
	factory, err := NewPebbleKVFactory(&FactoryOptions{DataDir: t.TempDir(), CacheSizeMB: 1})
	require.NoError(t, err)
	defer factory.Close()

	kv, err := factory.NewKV("", 1, proto.KeySortingType_NATURAL)

	assert.ErrorContains(t, err, "namespace must not be empty")
	assert.Nil(t, kv)
}

func TestPebbleFactoryNewSnapshotLoaderRejectsEmptyNamespace(t *testing.T) {
	factory, err := NewPebbleKVFactory(&FactoryOptions{DataDir: t.TempDir(), CacheSizeMB: 1})
	require.NoError(t, err)
	defer factory.Close()

	loader, err := factory.NewSnapshotLoader(context.Background(), "", 1, SnapshotLoaderOptions{})

	assert.ErrorContains(t, err, "namespace must not be empty")
	assert.Nil(t, loader)
}

func TestPebbleRangeScanNoLimits(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_HIERARCHICAL)
	assert.NoError(t, err)

	wb := kv.NewWriteBatch()
	assert.NoError(t, wb.Put("/root/a", []byte("a")))
	assert.NoError(t, wb.Put("/root/b", []byte("b")))
	assert.NoError(t, wb.Put("/root/c", []byte("c")))
	assert.NoError(t, wb.Put("/root/d", []byte("d")))
	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	// No max
	it, err := kv.RangeScan("/root/b", "", NoInternalKeys)
	assert.NoError(t, err)

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/b", it.Key())
	value, err := it.Value()
	assert.NoError(t, err)
	assert.Equal(t, "b", string(value))
	assert.True(t, it.Next())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/c", it.Key())
	value, err = it.Value()
	assert.NoError(t, err)
	assert.Equal(t, "c", string(value))
	assert.True(t, it.Next())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/d", it.Key())
	value, err = it.Value()
	assert.NoError(t, err)
	assert.Equal(t, "d", string(value))
	assert.False(t, it.Next())

	assert.False(t, it.Valid())

	assert.NoError(t, it.Close())

	// No min
	it, err = kv.RangeScan("", "/root/c", NoInternalKeys)
	assert.NoError(t, err)

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/a", it.Key())
	value, err = it.Value()
	assert.NoError(t, err)
	assert.Equal(t, "a", string(value))
	assert.True(t, it.Next())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/b", it.Key())
	value, err = it.Value()
	assert.NoError(t, err)
	assert.Equal(t, "b", string(value))
	assert.False(t, it.Next())

	assert.False(t, it.Valid())

	assert.NoError(t, it.Close())

	// No min and max
	it, err = kv.RangeScan("", "", NoInternalKeys)
	assert.NoError(t, err)

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/a", it.Key())
	value, err = it.Value()
	assert.NoError(t, err)
	assert.Equal(t, "a", string(value))
	assert.True(t, it.Next())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/b", it.Key())
	value, err = it.Value()
	assert.NoError(t, err)
	assert.Equal(t, "b", string(value))
	assert.True(t, it.Next())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/c", it.Key())
	value, err = it.Value()
	assert.NoError(t, err)
	assert.Equal(t, "c", string(value))
	assert.True(t, it.Next())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/d", it.Key())
	value, err = it.Value()
	assert.NoError(t, err)
	assert.Equal(t, "d", string(value))
	assert.False(t, it.Next())

	assert.False(t, it.Valid())

	assert.NoError(t, it.Close())

	assert.NoError(t, kv.Close())
	assert.NoError(t, factory.Close())
}

func TestPebbleReverseRangeScanNoLimits(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_HIERARCHICAL)
	assert.NoError(t, err)

	wb := kv.NewWriteBatch()
	assert.NoError(t, wb.Put("/root/a", []byte("a")))
	assert.NoError(t, wb.Put("/root/b", []byte("b")))
	assert.NoError(t, wb.Put("/root/c", []byte("c")))
	assert.NoError(t, wb.Put("/root/d", []byte("d")))
	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	// No max
	it, err := kv.KeyRangeScanReverse("/root/b", "", NoInternalKeys)
	assert.NoError(t, err)

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/d", it.Key())
	assert.True(t, it.Prev())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/c", it.Key())
	assert.True(t, it.Prev())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/b", it.Key())
	assert.False(t, it.Prev())

	assert.False(t, it.Valid())

	assert.NoError(t, it.Close())

	// No min
	it, err = kv.KeyRangeScanReverse("", "/root/c", NoInternalKeys)
	assert.NoError(t, err)

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/b", it.Key())
	assert.True(t, it.Prev())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/a", it.Key())
	assert.False(t, it.Prev())

	assert.False(t, it.Valid())

	assert.NoError(t, it.Close())

	// No min and max
	it, err = kv.KeyRangeScanReverse("", "", NoInternalKeys)
	assert.NoError(t, err)

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/d", it.Key())
	assert.True(t, it.Prev())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/c", it.Key())
	assert.True(t, it.Prev())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/b", it.Key())
	assert.True(t, it.Prev())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/a", it.Key())
	assert.False(t, it.Prev())

	assert.False(t, it.Valid())

	assert.NoError(t, it.Close())

	assert.NoError(t, kv.Close())
	assert.NoError(t, factory.Close())
}

func TestPebbbleFloorCeiling(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
	assert.NoError(t, err)

	key, res, closer, err := kv.Get("e", ComparisonFloor, NoInternalKeys)
	assert.ErrorIs(t, err, errs.ErrDBKeyNotFound)
	assert.Equal(t, "", key)
	assert.Nil(t, res)
	assert.Nil(t, closer)

	key, res, closer, err = kv.Get("e", ComparisonCeiling, NoInternalKeys)
	assert.ErrorIs(t, err, errs.ErrDBKeyNotFound)
	assert.Equal(t, "", key)
	assert.Nil(t, res)
	assert.Nil(t, closer)

	key, res, closer, err = kv.Get("e", ComparisonLower, NoInternalKeys)
	assert.ErrorIs(t, err, errs.ErrDBKeyNotFound)
	assert.Equal(t, "", key)
	assert.Nil(t, res)
	assert.Nil(t, closer)

	key, res, closer, err = kv.Get("e", ComparisonHigher, NoInternalKeys)
	assert.ErrorIs(t, err, errs.ErrDBKeyNotFound)
	assert.Equal(t, "", key)
	assert.Nil(t, res)
	assert.Nil(t, closer)

	// ---------------------------------------------------------------

	wb := kv.NewWriteBatch()
	assert.NoError(t, wb.Put("a", []byte("0")))
	// assert.NoError(t, wb.Put("b", []byte("1"))) // Intentionally skipped
	assert.NoError(t, wb.Put("c", []byte("2")))
	assert.NoError(t, wb.Put("d", []byte("3")))
	assert.NoError(t, wb.Put("e", []byte("4")))
	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	key, res, closer, err = kv.Get("a", ComparisonEqual, NoInternalKeys)
	assert.NoError(t, err)
	assert.Equal(t, "a", key)
	assert.Equal(t, "0", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("a", ComparisonFloor, NoInternalKeys)
	assert.NoError(t, err)
	assert.Equal(t, "a", key)
	assert.Equal(t, "0", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("a", ComparisonCeiling, NoInternalKeys)
	assert.NoError(t, err)
	assert.Equal(t, "a", key)
	assert.Equal(t, "0", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("a", ComparisonLower, NoInternalKeys)
	assert.ErrorIs(t, err, errs.ErrDBKeyNotFound)
	assert.Equal(t, "", key)
	assert.Nil(t, res)
	assert.Nil(t, closer)

	key, res, closer, err = kv.Get("a", ComparisonHigher, NoInternalKeys)
	assert.NoError(t, err)
	assert.Equal(t, "c", key)
	assert.Equal(t, "2", string(res))
	assert.NoError(t, closer.Close())

	// ---------------------------------------------------------------
	key, res, closer, err = kv.Get("b", ComparisonEqual, NoInternalKeys)
	assert.ErrorIs(t, err, errs.ErrDBKeyNotFound)
	assert.Equal(t, "", key)
	assert.Nil(t, res)
	assert.Nil(t, closer)

	key, res, closer, err = kv.Get("b", ComparisonFloor, NoInternalKeys)
	assert.NoError(t, err)
	assert.Equal(t, "a", key)
	assert.Equal(t, "0", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("b", ComparisonCeiling, NoInternalKeys)
	assert.NoError(t, err)
	assert.Equal(t, "c", key)
	assert.Equal(t, "2", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("b", ComparisonLower, NoInternalKeys)
	assert.NoError(t, err)
	assert.Equal(t, "a", key)
	assert.Equal(t, "0", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("b", ComparisonHigher, NoInternalKeys)
	assert.NoError(t, err)
	assert.Equal(t, "c", key)
	assert.Equal(t, "2", string(res))
	assert.NoError(t, closer.Close())

	// ---------------------------------------------------------------

	key, res, closer, err = kv.Get("c", ComparisonFloor, NoInternalKeys)
	assert.NoError(t, err)
	assert.Equal(t, "c", key)
	assert.Equal(t, "2", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("c", ComparisonCeiling, NoInternalKeys)
	assert.NoError(t, err)
	assert.Equal(t, "c", key)
	assert.Equal(t, "2", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("c", ComparisonLower, NoInternalKeys)
	assert.NoError(t, err)
	assert.Equal(t, "a", key)
	assert.Equal(t, "0", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("c", ComparisonHigher, NoInternalKeys)
	assert.NoError(t, err)
	assert.Equal(t, "d", key)
	assert.Equal(t, "3", string(res))
	assert.NoError(t, closer.Close())

	// ---------------------------------------------------------------

	key, res, closer, err = kv.Get("d", ComparisonFloor, NoInternalKeys)
	assert.NoError(t, err)
	assert.Equal(t, "d", key)
	assert.Equal(t, "3", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("d", ComparisonCeiling, NoInternalKeys)
	assert.NoError(t, err)
	assert.Equal(t, "d", key)
	assert.Equal(t, "3", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("d", ComparisonLower, NoInternalKeys)
	assert.NoError(t, err)
	assert.Equal(t, "c", key)
	assert.Equal(t, "2", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("d", ComparisonHigher, NoInternalKeys)
	assert.NoError(t, err)
	assert.Equal(t, "e", key)
	assert.Equal(t, "4", string(res))
	assert.NoError(t, closer.Close())

	// ---------------------------------------------------------------

	key, res, closer, err = kv.Get("e", ComparisonFloor, NoInternalKeys)
	assert.NoError(t, err)
	assert.Equal(t, "e", key)
	assert.Equal(t, "4", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("e", ComparisonCeiling, NoInternalKeys)
	assert.NoError(t, err)
	assert.Equal(t, "e", key)
	assert.Equal(t, "4", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("e", ComparisonLower, NoInternalKeys)
	assert.NoError(t, err)
	assert.Equal(t, "d", key)
	assert.Equal(t, "3", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("e", ComparisonHigher, NoInternalKeys)
	assert.ErrorIs(t, err, errs.ErrDBKeyNotFound)
	assert.Equal(t, "", key)
	assert.Nil(t, res)
	assert.Nil(t, closer)

	assert.NoError(t, kv.Close())
	assert.NoError(t, factory.Close())
}

// Nothing sorts below the empty key: its floor is "" itself when it exists, and
// its lower is never found.
func TestPebbleGetEmptyKey(t *testing.T) {
	const notFound = "<not found>"

	for _, keySorting := range []proto.KeySortingType{proto.KeySortingType_NATURAL, proto.KeySortingType_HIERARCHICAL} {
		t.Run(keySorting.String(), func(t *testing.T) {
			factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
			require.NoError(t, err)
			kv, err := factory.NewKV(constant.DefaultNamespace, 1, keySorting)
			require.NoError(t, err)

			lookup := func(comparison ComparisonType) string {
				key, err := getKey(t, kv, "", comparison, NoInternalKeys)
				if errors.Is(err, errs.ErrDBKeyNotFound) {
					return notFound
				}
				assert.NoError(t, err)
				return key
			}

			putAll(t, kv, "a", "b", "c")
			assert.Equal(t, notFound, lookup(ComparisonFloor))
			assert.Equal(t, notFound, lookup(ComparisonLower))
			assert.Equal(t, "a", lookup(ComparisonCeiling))
			assert.Equal(t, "a", lookup(ComparisonHigher))

			putAll(t, kv, "")
			assert.Equal(t, "", lookup(ComparisonFloor))
			assert.Equal(t, notFound, lookup(ComparisonLower))
			assert.Equal(t, "", lookup(ComparisonCeiling))
			assert.Equal(t, "a", lookup(ComparisonHigher))

			assert.NoError(t, kv.Close())
			assert.NoError(t, factory.Close())
		})
	}
}

// The natural encoder encodes "" to an empty slice, which is nil when the "" has
// no data pointer, as when it is decoded from a request, and may not be nil
// otherwise. The lookups must take both for the empty key.
func TestPebbleGetEmptyEncodedKey(t *testing.T) {
	const notFound = "<not found>"

	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
	require.NoError(t, err)
	p := kv.(*Pebble)

	lookups := map[ComparisonType]func([]byte, IteratorOpts) (string, []byte, io.Closer, error){
		ComparisonFloor:   p.getFloor,
		ComparisonLower:   p.getLower,
		ComparisonCeiling: p.getCeiling,
		ComparisonHigher:  p.getHigher,
	}
	lookup := func(key []byte, comparison ComparisonType) string {
		found, _, closer, err := lookups[comparison](key, NoInternalKeys)
		if errors.Is(err, pebble.ErrNotFound) {
			return notFound
		}
		require.NoError(t, err)
		assert.NoError(t, closer.Close())
		return found
	}

	putAll(t, kv, "a", "b", "c")
	for _, key := range [][]byte{nil, {}} {
		assert.Equal(t, notFound, lookup(key, ComparisonFloor))
		assert.Equal(t, notFound, lookup(key, ComparisonLower))
		assert.Equal(t, "a", lookup(key, ComparisonCeiling))
		assert.Equal(t, "a", lookup(key, ComparisonHigher))
	}

	putAll(t, kv, "")
	for _, key := range [][]byte{nil, {}} {
		assert.Equal(t, "", lookup(key, ComparisonFloor))
		assert.Equal(t, notFound, lookup(key, ComparisonLower))
		assert.Equal(t, "", lookup(key, ComparisonCeiling))
		assert.Equal(t, "a", lookup(key, ComparisonHigher))
	}

	assert.NoError(t, kv.Close())
	assert.NoError(t, factory.Close())
}

func TestPebbleFindLowerInBatch(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
	assert.NoError(t, err)

	wb := kv.NewWriteBatch()
	assert.NoError(t, wb.Put("a", []byte("a")))
	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	wb = kv.NewWriteBatch()
	assert.NoError(t, wb.Put("b", []byte("b")))
	assert.NoError(t, wb.Put("c", []byte("c")))

	key, err := wb.FindLower("a")
	assert.ErrorIs(t, err, errs.ErrDBKeyNotFound)
	assert.Equal(t, "", key)

	key, err = wb.FindLower("b")
	assert.NoError(t, err)
	assert.Equal(t, "a", key)

	key, err = wb.FindLower("c")
	assert.NoError(t, err)
	assert.Equal(t, "b", key)

	key, err = wb.FindLower("d")
	assert.NoError(t, err)
	assert.Equal(t, "c", key)

	key, err = wb.FindLower("z")
	assert.NoError(t, err)
	assert.Equal(t, "c", key)

	assert.NoError(t, kv.Close())
	assert.NoError(t, factory.Close())
}

func TestPebbleGetAndFindLowerReadFailure(t *testing.T) {
	kv := newKVWithUnreadableSST(t)

	for _, c := range []struct {
		comparisonType ComparisonType
		key            string
	}{
		// The exact lookup of FLOOR reads the unreadable sstable
		{ComparisonFloor, "c"},
		// "e" is past both sstables: the exact lookup of FLOOR reads neither
		{ComparisonFloor, "e"},
		{ComparisonCeiling, "c"},
		{ComparisonLower, "e"},
		{ComparisonHigher, "c"},
		// Lands on "b" in the readable sstable, then fails to step to "c"
		{ComparisonHigher, "b"},
	} {
		_, _, _, err := kv.Get(c.key, c.comparisonType, NoInternalKeys)
		assert.ErrorIs(t, err, os.ErrPermission, "comparison type %d, key %q", c.comparisonType, c.key)
	}

	wb := kv.NewWriteBatch()
	defer wb.Close()
	_, err := wb.FindLower("e")
	assert.ErrorIs(t, err, os.ErrPermission)
	assert.NotErrorIs(t, err, errs.ErrDBKeyNotFound)
}

func TestPebbleScanReadFailure(t *testing.T) {
	kv := newKVWithUnreadableSST(t)

	for _, test := range []struct {
		name string
		more bool
	}{
		// Fails to step from "b" to "c"
		{"all", true},
		// Fails to find whether "b" is the last key
		{"first", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			var visited []string
			_, err := kv.Scan(nil, func(key string, _ []byte) (bool, error) {
				visited = append(visited, key)
				return test.more, nil
			})
			assert.ErrorIs(t, err, os.ErrPermission)
			assert.Equal(t, []string{"b"}, visited)
		})
	}
}

func TestPebbleBatchGetReadFailure(t *testing.T) {
	kv := newKVWithUnreadableSST(t)
	wb := kv.NewWriteBatch()
	defer wb.Close()

	// The first Get of a batch reads through the batch
	_, _, err := wb.Get("c")
	assert.ErrorIs(t, err, os.ErrPermission)
	// The next ones through an iterator, which keeps the failure
	_, _, err = wb.Get("d")
	assert.ErrorIs(t, err, os.ErrPermission)
	assert.ErrorIs(t, wb.Commit(), os.ErrPermission)
}

func TestPebbleScanFailsWhenValueCannotBeRead(t *testing.T) {
	kv := newKVWithUnreadableValueBlocks(t)

	var visited []string
	_, err := kv.Scan(nil, func(key string, _ []byte) (bool, error) {
		visited = append(visited, key)
		return true, nil
	})
	assert.ErrorIs(t, err, errorfs.ErrInjected)
	assert.Equal(t, []string{"a"}, visited)
}

func TestPebbleBatchGetFailsWhenValueCannotBeRead(t *testing.T) {
	kv := newKVWithUnreadableValueBlocks(t)
	wb := kv.NewWriteBatch()

	// The first Get of a batch reads through the batch, the next ones through
	// an iterator
	value, closer, err := wb.Get("a")
	require.NoError(t, err)
	assert.Equal(t, []byte("value-a"), value)
	assert.NoError(t, closer.Close())
	_, _, err = wb.Get("b")
	assert.ErrorIs(t, err, errorfs.ErrInjected)
	assert.ErrorIs(t, wb.Close(), errorfs.ErrInjected)
}

func TestPebbleIteratorValueFailsWhenValueCannotBeRead(t *testing.T) {
	kv := newKVWithUnreadableValueBlocks(t)
	it, err := kv.RangeScan("b", "", NoInternalKeys)
	require.NoError(t, err)

	require.True(t, it.Valid())
	_, err = it.Value()
	assert.ErrorIs(t, err, errorfs.ErrInjected)
	assert.ErrorIs(t, it.Close(), errorfs.ErrInjected)
}

func TestPebbleReverseIteratorValueFailsWhenValueCannotBeRead(t *testing.T) {
	kv := newKVWithUnreadableValueBlocks(t)
	it, err := kv.KeyRangeScanReverse("a", "", NoInternalKeys)
	require.NoError(t, err)

	require.True(t, it.Valid())
	_, err = it.(*PebbleReverseIterator).Value()
	assert.ErrorIs(t, err, errorfs.ErrInjected)
	assert.ErrorIs(t, it.Close(), errorfs.ErrInjected)
}

func TestPebblePrefixIteratorLookupFailsWhenValueCannotBeRead(t *testing.T) {
	kv := newKVWithUnreadableValueBlocks(t)
	it, err := kv.KeyPrefixIterator("", "")
	require.NoError(t, err)

	value, err := it.Lookup("a")
	assert.NoError(t, err)
	assert.Equal(t, []byte("value-a"), value)
	_, err = it.Lookup("b")
	assert.ErrorIs(t, err, errorfs.ErrInjected)
	assert.ErrorIs(t, it.Close(), errorfs.ErrInjected)
}

// Pebble rejects a synced write without a WAL, the one commit failure that
// it returns rather than reporting through the logger's Fatalf.
func TestPebbleBatchCommitFailsWhenPebbleRejectsTheBatch(t *testing.T) {
	options := NewFactoryOptionsForTest(t)
	options.SyncData = true
	factory, err := NewPebbleKVFactory(options)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, factory.Close()) })
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, kv.Close()) })

	wb := kv.NewWriteBatch()
	defer wb.Close()
	require.NoError(t, wb.Put("a", []byte("a")))
	assert.ErrorContains(t, wb.Commit(), "WAL disabled")
}

func toList(it KeyIterator) []string {
	var list []string

	for it.Valid() {
		list = append(list, it.Key())
		it.Next()
	}

	it.Close()
	return list
}

func TestPebbleHierarchicalScanStop(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	assert.NoError(t, err)

	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_HIERARCHICAL)
	assert.NoError(t, err)

	wb := kv.NewWriteBatch()
	keys := []string{"/a", "/b", "/a/a-1", "/a/a-2", "/b/c", "/b/c/b", "/b/c/b/1"}

	for _, key := range keys {
		assert.NoError(t, wb.Put(key, []byte(key)))
	}

	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	it, _ := kv.KeyRangeScan("/", "//", NoInternalKeys)
	assert.Equal(t, []string{"/a", "/b"}, toList(it))

	it, _ = kv.KeyRangeScan("/a/", "/a//", NoInternalKeys)
	assert.Equal(t, []string{"/a/a-1", "/a/a-2"}, toList(it))

	it, _ = kv.KeyRangeScan("/b/", "/b//", NoInternalKeys)
	assert.Equal(t, []string{"/b/c"}, toList(it))

	assert.NoError(t, kv.Close())
	assert.NoError(t, factory.Close())
}

func TestPebbleBatchChecksum(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
	assert.NoError(t, err)

	// Test that checksum is computed from batch content
	wb1 := kv.NewWriteBatch()
	assert.NoError(t, wb1.Put("key1", []byte("value1")))

	wb2 := kv.NewWriteBatch()
	assert.NoError(t, wb2.Put("key1", []byte("value1")))

	// Same content should produce same checksum
	cs1 := wb1.Checksum(crc.Checksum(0))
	cs2 := wb2.Checksum(crc.Checksum(0))
	assert.Equal(t, cs1, cs2, "identical batches should produce same checksum")

	// Different content should produce different checksum
	wb3 := kv.NewWriteBatch()
	assert.NoError(t, wb3.Put("key1", []byte("value2")))

	cs3 := wb3.Checksum(crc.Checksum(0))
	assert.NotEqual(t, cs1, cs3, "different content should produce different checksum")

	// Additional operations should change checksum
	assert.NoError(t, wb1.Put("key2", []byte("value2")))
	cs4 := wb1.Checksum(crc.Checksum(0))
	assert.NotEqual(t, cs1, cs4, "additional operations should change checksum")

	// Test that initial checksum affects result
	cs5 := wb2.Checksum(crc.Checksum(100))
	assert.NotEqual(t, cs2, cs5, "different initial checksum should produce different result")

	assert.NoError(t, wb1.Close())
	assert.NoError(t, wb2.Close())
	assert.NoError(t, wb3.Close())
	assert.NoError(t, kv.Close())
	assert.NoError(t, factory.Close())
}

func TestPebbleBatchChecksumChaining(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
	assert.NoError(t, err)

	// Simulate chaining checksums across batches (like the database fingerprint feature)
	var runningChecksum crc.Checksum

	wb1 := kv.NewWriteBatch()
	assert.NoError(t, wb1.Put("a", []byte("1")))
	cs1 := wb1.Checksum(runningChecksum)
	runningChecksum = cs1
	assert.NoError(t, wb1.Commit())
	assert.NoError(t, wb1.Close())

	wb2 := kv.NewWriteBatch()
	assert.NoError(t, wb2.Put("b", []byte("2")))
	cs2 := wb2.Checksum(runningChecksum)
	assert.NoError(t, wb2.Commit())
	assert.NoError(t, wb2.Close())

	// Repeat the same sequence and verify we get the same final checksum
	var runningChecksum2 crc.Checksum

	wb3 := kv.NewWriteBatch()
	assert.NoError(t, wb3.Put("a", []byte("1")))
	cs3 := wb3.Checksum(runningChecksum2)
	runningChecksum2 = cs3
	assert.NoError(t, wb3.Close())

	wb4 := kv.NewWriteBatch()
	assert.NoError(t, wb4.Put("b", []byte("2")))
	cs4 := wb4.Checksum(runningChecksum2)
	assert.NoError(t, wb4.Close())

	assert.Equal(t, cs2, cs4, "same sequence of operations should produce same final checksum")

	assert.NoError(t, kv.Close())
	assert.NoError(t, factory.Close())
}

func TestPebbleRejectsInvalidNamespace(t *testing.T) {
	options := NewFactoryOptionsForTest(t)
	factory, err := NewPebbleKVFactory(options)
	assert.NoError(t, err)

	_, err = factory.NewKV("../escaped-ns", 7, proto.KeySortingType_HIERARCHICAL)
	assert.ErrorContains(t, err, "invalid path traversal sequence")

	_, err = factory.NewSnapshotLoader(context.Background(), "../escaped-ns", 7, SnapshotLoaderOptions{})
	assert.ErrorContains(t, err, "invalid path traversal sequence")

	// The data dir is a t.TempDir(), so its parent is the per-test temp root
	_, err = os.Stat(filepath.Join(filepath.Dir(options.DataDir), "escaped-ns"))
	assert.ErrorIs(t, err, os.ErrNotExist)

	assert.NoError(t, factory.Close())
}

// The snapshots left behind by a previous run are removed at startup. The
// "snapshots" name is reserved, so the whole directory can go.
func TestPebbleCleanupSnapshots(t *testing.T) {
	options := NewFactoryOptionsForTest(t)

	snapshotsDir := filepath.Join(options.DataDir, validation.KeywordNamespaceSnapshots)
	require.NoError(t, os.MkdirAll(filepath.Join(snapshotsDir, "shard-1", "snapshot-1"), 0o755))

	// A data server restart creates a new factory on the same data dir
	factory, err := NewPebbleKVFactory(options)
	require.NoError(t, err)

	_, err = os.Stat(snapshotsDir)
	assert.True(t, os.IsNotExist(err), "the leftover snapshot should have been removed")

	assert.NoError(t, factory.Close())
}

func TestCompareWithDataset(t *testing.T) {
	for _, test := range []struct {
		leftKey  string
		rightKey string
		expect   int
	}{
		{"aaa", "aaa", 0},
		{"aaa", "zzzzz", -1},
		{"bbbbb", "aaaaa", +1},
		{"aaaaa", "", +1},
		{"", "aaaaaa", -1},
		{"", "", 0},
		{"aaaaa", "aaaaaaaaaaa", -1},
		{"aaaaaaaaaaa", "aaa", +1},
		{"a", "/", -1},
		{"/", "a", +1},
		{"/aaaa", "/bbbbb", -1},
		{"/aaaa", "/aa/a", -1},
		{"/aaaa/a", "/aaaa/b", -1},
		{"/aaaa/a/a", "/bbbbbbbbbb", +1},
		{"/aaaa/a/a", "/aaaa/bbbbbbbbbb", +1},
		{"/a/b/a/a/a", "/a/b/a/b", +1},

		{"/a", "/b", -1},
		{"/a", "/a/", -1},
		{"/a/", "/a//", -1},
		{"/a/a-2", "/b/c", -1},
		{"/", "/a", -1},
		{"/a", "//", -1},
		{"/b", "//", -1},
		{"//", "/a/a-1", -1},

		{"//", "/a/a-1", -1},

		{"/a/a-1", "/a//", -1},
		{"/a//", "/b/c", -1},
	} {
		t.Run(fmt.Sprintf("%v?%v", test.leftKey, test.rightKey), func(t *testing.T) {
			// test compare with slash
			assert.Equal(t, test.expect, compare.CompareWithSlash([]byte(test.leftKey), []byte(test.rightKey)))
			// test compare with abbreviated key
			lbk := AbbreviatedKeyDisableSlash([]byte(test.leftKey))
			rbk := AbbreviatedKeyDisableSlash([]byte(test.rightKey))
			if lbk == rbk {
				assert.Equal(t, test.expect, compare.CompareWithSlash([]byte(test.leftKey), []byte(test.rightKey)))
			} else {
				assert.Equal(t, test.expect, cmp.Compare(lbk, rbk))
			}

			// Test encoded compare to ensure we are going to
			// have the same exact sorting
			encodedLeft := compare.EncoderHierarchical.Encode(test.leftKey)
			encodedRight := compare.EncoderHierarchical.Encode(test.rightKey)
			assert.Equal(t, test.expect, bytes.Compare(encodedLeft, encodedRight))
		})
	}
}

func TestNewPebbleKVFactoryUsesDefaultOptions(t *testing.T) {
	t.Chdir(t.TempDir())
	snapshotsDir := filepath.Join(DefaultFactoryOptions.DataDir, validation.KeywordNamespaceSnapshots)
	require.NoError(t, os.MkdirAll(filepath.Join(snapshotsDir, "shard-1"), 0o755))

	factory, err := NewPebbleKVFactory(nil)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, factory.Close()) })

	assert.Equal(t, DefaultFactoryOptions.DataDir, factory.(*PebbleFactory).dataDir)
	assert.NoDirExists(t, snapshotsDir)
}

func TestNewPebbleKVFactoryFailsWhenSnapshotsCannotBeRemoved(t *testing.T) {
	options := NewFactoryOptionsForTest(t)
	snapshotDir := filepath.Join(options.DataDir, validation.KeywordNamespaceSnapshots, "shard-1")
	require.NoError(t, os.MkdirAll(snapshotDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(snapshotDir, "file"), nil, 0o644))
	require.NoError(t, os.Chmod(snapshotDir, 0o500))
	t.Cleanup(func() { assert.NoError(t, os.Chmod(snapshotDir, 0o700)) })

	_, err := NewPebbleKVFactory(options)
	assert.ErrorIs(t, err, os.ErrPermission)
	assert.ErrorContains(t, err, "failed to delete database snapshots")
}

func TestPebbleFactoryTakeMemTableSlotFailsWhenAllTaken(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, factory.Close()) })
	pf := factory.(*PebbleFactory)

	for i := 0; i < maxMemTableSlots; i++ {
		require.True(t, pf.takeMemTableSlot())
	}
	assert.False(t, pf.takeMemTableSlot())

	pf.returnMemTableSlot()
	assert.True(t, pf.takeMemTableSlot())
	for i := 0; i < maxMemTableSlots; i++ {
		pf.returnMemTableSlot()
	}
}

func TestPebbleNewKVFailsWhenMarkerCannotBeCreated(t *testing.T) {
	options := NewFactoryOptionsForTest(t)
	// A file in place of the namespace directory
	require.NoError(t, os.WriteFile(filepath.Join(options.DataDir, constant.DefaultNamespace), nil, 0o644))
	factory, err := NewPebbleKVFactory(options)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, factory.Close()) })

	_, err = factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
	assert.ErrorContains(t, err, "failed to create marker")
}

func TestPebbleGetPanicsOnUnknownComparison(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, factory.Close()) })
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, kv.Close()) })

	assert.PanicsWithValue(t, "Unknown comparison type: 99", func() {
		_, _, _, _ = kv.Get("a", ComparisonType(99), NoInternalKeys)
	})
}

func TestPebbleSeekRejectsUnknownComparison(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, factory.Close()) })
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, kv.Close()) })

	_, err = kv.Seek("a", ComparisonEqual)
	assert.ErrorContains(t, err, "unsupported comparison type")
}

func TestPebbleReverseIteratorValue(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, factory.Close()) })
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, kv.Close()) })
	wb := kv.NewWriteBatch()
	require.NoError(t, wb.Put("a", []byte("value-a")))
	require.NoError(t, wb.Put("b", []byte("value-b")))
	require.NoError(t, wb.Commit())
	require.NoError(t, wb.Close())

	it, err := kv.KeyRangeScanReverse("a", "c", NoInternalKeys)
	require.NoError(t, err)
	reverse := it.(*PebbleReverseIterator)
	var values []string
	for ; reverse.Valid(); reverse.Prev() {
		value, err := reverse.Value()
		require.NoError(t, err)
		values = append(values, string(value))
	}
	assert.Equal(t, []string{"value-b", "value-a"}, values)
	assert.NoError(t, it.Close())
}

func TestPebbleBatchPutMarshalableFailsWhenMarshalingFails(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, factory.Close()) })
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, kv.Close()) })

	marshalErr := errors.New("marshal failed")
	wb := kv.NewWriteBatch()
	err = wb.PutMarshalable("k", &fakeMarshalable{size: 4, marshal: func([]byte) (int, error) { return 0, marshalErr }})
	assert.ErrorIs(t, err, marshalErr)
	assert.NoError(t, wb.Close())
}

func TestPebbleBatchPutMarshalableFailsWhenMarshalingFillsLessThanItsSize(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, factory.Close()) })
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, kv.Close()) })

	wb := kv.NewWriteBatch()
	err = wb.PutMarshalable("k", &fakeMarshalable{size: 4, marshal: func(b []byte) (int, error) { return copy(b, "ab"), nil }})
	assert.ErrorContains(t, err, "does not match the reserved record size")
	assert.NoError(t, wb.Close())
}

func TestPebbleSnapshotLoaderAddChunkRejectsUnfinishedFile(t *testing.T) {
	factory := newMemFactory(t, memFSWithRoom(), nil)
	loader, err := factory.NewSnapshotLoader(context.Background(), constant.DefaultNamespace, 1, SnapshotLoaderOptions{})
	require.NoError(t, err)

	require.NoError(t, loader.AddChunk("a", 0, 2, []byte("first")))
	err = loader.AddChunk("b", 0, 1, []byte("other"))
	assert.ErrorContains(t, err, "previous file not finished")
	assert.NoError(t, loader.Close())
}

func TestPebbleSnapshotLoaderFailsOnFilesystemErrors(t *testing.T) {
	paths := newInstallPaths(filepath.Join("/data", constant.DefaultNamespace, "shard-1"))
	parent := filepath.Dir(paths.db)
	addChunk := func(loader SnapshotLoader) error { return loader.AddChunk("a", 0, 1, []byte("content")) }
	install := func(loader SnapshotLoader) error {
		return multierr.Append(addChunk(loader), loader.Install(noCheck, nil, noLogClear))
	}
	swapped := opIs(errorfs.OpRename, paths.stage)
	for _, test := range []struct {
		name   string
		inject func(op errorfs.Op) bool
		// The step that fails, after the loader got created
		step func(loader SnapshotLoader) error
	}{
		{"stat-marker", opIs(errorfs.OpStat, paths.marker), nil},
		{"remove-stage", opIs(errorfs.OpRemoveAll, paths.stage), nil},
		{"create-stage", opIs(errorfs.OpMkdirAll, paths.stage), nil},
		{"read-disk-usage", opIs(errorfs.OpGetDiskUsage, ""), nil},
		{"create-file", opIs(errorfs.OpCreate, filepath.Join(paths.stage, "a")), addChunk},
		{"write-file", opIs(errorfs.OpFileWrite, ""), addChunk},
		{"sync-file", func(op errorfs.Op) bool {
			return op.Kind == errorfs.OpFileSync || op.Kind == errorfs.OpFileSyncData || op.Kind == errorfs.OpFileSyncTo
		}, addChunk},
		{"sync-stage", opIs(errorfs.OpOpenDir, paths.stage), install},
		{"create-marker", opIs(errorfs.OpCreate, paths.marker), install},
		{"stat-stage", opIs(errorfs.OpStat, paths.stage), install},
		{"remove-leftover", opIs(errorfs.OpRemoveAll, paths.replaced), install},
		{"move-database-aside", opIs(errorfs.OpRename, paths.db), install},
		{"move-stage", opIs(errorfs.OpRename, paths.stage), install},
		{"sync-swap", afterOp(swapped, opIs(errorfs.OpOpenDir, parent)), install},
		{"remove-replaced", afterOp(swapped, opIs(errorfs.OpRemoveAll, paths.replaced)), install},
		{"unmark", opIs(errorfs.OpRemove, paths.marker), install},
	} {
		t.Run(test.name, func(t *testing.T) {
			mem := memFSWithRoom()
			require.NoError(t, mem.MkdirAll(paths.db, 0755))
			fs := errorfs.Wrap(mem, errorfs.InjectorFunc(func(op errorfs.Op) error {
				if test.inject(op) {
					return errorfs.ErrInjected
				}
				return nil
			}))
			factory := newMemFactory(t, fs, nil)

			loader, err := factory.NewSnapshotLoader(context.Background(), constant.DefaultNamespace, 1, SnapshotLoaderOptions{})
			if test.step == nil {
				assert.ErrorIs(t, err, errorfs.ErrInjected)
				return
			}
			require.NoError(t, err)
			assert.ErrorIs(t, test.step(loader), errorfs.ErrInjected)
			assert.NoError(t, loader.Close())
		})
	}
}

func TestPebbleRecoverSnapshotInstallFailsOnFilesystemErrors(t *testing.T) {
	paths := newInstallPaths(filepath.Join("/data", constant.DefaultNamespace, "shard-1"))
	for _, test := range []struct {
		name   string
		marked bool
		inject func(op errorfs.Op) bool
	}{
		{"open-marker", true, opIs(errorfs.OpOpen, paths.marker)},
		{"read-marker", true, opIs(errorfs.OpFileRead, paths.marker)},
		{"remove-stage", false, opIs(errorfs.OpRemoveAll, paths.stage)},
	} {
		t.Run(test.name, func(t *testing.T) {
			mem := memFSWithRoom()
			require.NoError(t, mem.MkdirAll(paths.stage, 0755))
			if test.marked {
				require.NoError(t, writeInstallMarker(mem, paths.marker, 1))
			}
			fs := errorfs.Wrap(mem, errorfs.InjectorFunc(func(op errorfs.Op) error {
				if test.inject(op) {
					return errorfs.ErrInjected
				}
				return nil
			}))
			factory := newMemFactory(t, fs, nil)

			err := factory.RecoverSnapshotInstall(constant.DefaultNamespace, 1, noLogClear)
			assert.ErrorIs(t, err, errorfs.ErrInjected)
		})
	}
}

func TestPebbleGetFailsWhenIteratorCannotBeCreated(t *testing.T) {
	for _, test := range []struct {
		name           string
		failpoint      string
		comparisonType ComparisonType
	}{
		{"ceiling", "getCeiling.newIter", ComparisonCeiling},
		{"lower", "getLower.newIter", ComparisonLower},
		{"floor", "getLower.newIter", ComparisonFloor},
		{"higher", "getHigher.newIter", ComparisonHigher},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, kv := newKVWithFailpoint(t, test.failpoint)

			_, _, closer, err := kv.Get("a", test.comparisonType, NoInternalKeys)
			assert.ErrorIs(t, err, errFailpoint)
			assert.Nil(t, closer)
			assert.NoError(t, kv.Close())
		})
	}
}

func TestPebbleKeyPrefixIteratorFailsWhenIteratorCannotBeCreated(t *testing.T) {
	_, kv := newKVWithFailpoint(t, "keyPrefixIterator.newIter")

	it, err := kv.KeyPrefixIterator("a", "")
	assert.ErrorIs(t, err, errFailpoint)
	assert.Nil(t, it)
	assert.NoError(t, kv.Close())
}

func TestPebbleKeyRangeScanReverseFailsWhenIteratorCannotBeCreated(t *testing.T) {
	_, kv := newKVWithFailpoint(t, "keyRangeScanReverse.newIter")

	it, err := kv.KeyRangeScanReverse("a", "b", NoInternalKeys)
	assert.ErrorIs(t, err, errFailpoint)
	assert.Nil(t, it)
	assert.NoError(t, kv.Close())
}

func TestPebbleRangeScanFailsWhenIteratorCannotBeCreated(t *testing.T) {
	_, kv := newKVWithFailpoint(t, "rangeScan.newIter")

	it, err := kv.RangeScan("a", "b", NoInternalKeys)
	assert.ErrorIs(t, err, errFailpoint)
	assert.Nil(t, it)
	assert.NoError(t, kv.Close())
}

func TestPebbleSeekFailsWhenIteratorCannotBeCreated(t *testing.T) {
	_, kv := newKVWithFailpoint(t, "seek.newIter")

	it, err := kv.Seek("a", ComparisonCeiling)
	assert.ErrorIs(t, err, errFailpoint)
	assert.Nil(t, it)
	assert.NoError(t, kv.Close())
}

func TestPebbleScanFailsWhenIteratorCannotBeCreated(t *testing.T) {
	_, kv := newKVWithFailpoint(t, "scan.newIter")

	next, err := kv.Scan(nil, func(string, []byte) (bool, error) {
		return true, nil
	})
	assert.ErrorIs(t, err, errFailpoint)
	assert.Nil(t, next)
	assert.NoError(t, kv.Close())
}

func TestPebbleBatchRangeScanFailsWhenIteratorCannotBeCreated(t *testing.T) {
	_, kv := newKVWithFailpoint(t, "batchRangeScan.newIter")
	wb := kv.NewWriteBatch()

	it, err := wb.RangeScan("a", "b")
	assert.ErrorIs(t, err, errFailpoint)
	assert.Nil(t, it)
	assert.NoError(t, wb.Close())
	assert.NoError(t, kv.Close())
}

func TestPebbleBatchPutFailsWhenSetFails(t *testing.T) {
	_, kv := newKVWithFailpoint(t, "batchPut.set")
	wb := kv.NewWriteBatch()

	assert.ErrorIs(t, wb.Put("a", []byte("value")), errFailpoint)
	assert.NoError(t, wb.Close())
	assert.NoError(t, kv.Close())
}

func TestPebbleBatchPutMarshalableFailsWhenFinishFails(t *testing.T) {
	_, kv := newKVWithFailpoint(t, "batchPutMarshalable.finish")
	wb := kv.NewWriteBatch()

	err := wb.PutMarshalable("a", &fakeMarshalable{size: 1, marshal: func(b []byte) (int, error) {
		return copy(b, "v"), nil
	}})
	assert.ErrorIs(t, err, errFailpoint)
	assert.NoError(t, wb.Close())
	assert.NoError(t, kv.Close())
}

func TestPebbleBatchDeleteFailsWhenDeleteFails(t *testing.T) {
	_, kv := newKVWithFailpoint(t, "batchDelete.delete")
	wb := kv.NewWriteBatch()

	assert.ErrorIs(t, wb.Delete("a"), errFailpoint)
	assert.NoError(t, wb.Close())
	assert.NoError(t, kv.Close())
}

func TestPebbleBatchGetFailsWhenIteratorCannotBeCreated(t *testing.T) {
	_, kv := newKVWithFailpoint(t, "batchGet.newIter")
	wb := kv.NewWriteBatch()

	// The first Get of a batch reads through the batch, the next ones through
	// an iterator
	_, _, err := wb.Get("a")
	assert.ErrorIs(t, err, errs.ErrDBKeyNotFound)
	_, closer, err := wb.Get("b")
	assert.ErrorIs(t, err, errFailpoint)
	assert.Nil(t, closer)
	assert.NoError(t, wb.Close())
	assert.NoError(t, kv.Close())
}

func TestPebbleBatchFindLowerFailsWhenIteratorCannotBeCreated(t *testing.T) {
	_, kv := newKVWithFailpoint(t, "batchFindLower.newIter")
	wb := kv.NewWriteBatch()

	_, err := wb.FindLower("b")
	assert.ErrorIs(t, err, errFailpoint)
	assert.NoError(t, wb.Close())
	assert.NoError(t, kv.Close())
}

func TestPebblePrefixIteratorLookupFailsWhenIteratorCannotBeCloned(t *testing.T) {
	_, kv := newKVWithFailpoint(t, "prefixIteratorLookup.clone")
	it, err := kv.KeyPrefixIterator("a", "")
	require.NoError(t, err)

	_, err = it.Lookup("a/b")
	assert.ErrorIs(t, err, errFailpoint)
	assert.NoError(t, it.Close())
	assert.NoError(t, kv.Close())
}

func TestCloseFailedIterWithoutIterator(t *testing.T) {
	assert.ErrorIs(t, closeFailedIter(nil, errFailpoint), errFailpoint)
}

// fakeMarshalable reserves size bytes, and fills them with marshal.
type fakeMarshalable struct {
	size    int
	marshal func(b []byte) (int, error)
}

func (m *fakeMarshalable) SizeVT() int {
	return m.size
}

func (m *fakeMarshalable) MarshalToSizedBufferVT(b []byte) (int, error) {
	return m.marshal(b)
}

// opIs matches the filesystem operations of the given kind, on the given path
// if any.
func opIs(kind errorfs.OpKind, path string) func(op errorfs.Op) bool {
	return func(op errorfs.Op) bool {
		return op.Kind == kind && (path == "" || op.Path == path)
	}
}

// newFactoryWithFaults returns a factory on dataDir whose filesystem fails the
// operations that inject matches with errorfs.ErrInjected.
func newFactoryWithFaults(t *testing.T, dataDir string, inject func(op errorfs.Op) bool) Factory {
	t.Helper()
	injector := errorfs.InjectorFunc(func(op errorfs.Op) error {
		if inject(op) {
			return errorfs.ErrInjected
		}
		return nil
	})
	options := NewFactoryOptionsForTest(t)
	options.DataDir = dataDir
	options.FS = closeFaultFS{FS: errorfs.Wrap(vfs.Default, injector), injector: injector}
	factory, err := NewPebbleKVFactory(options)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, factory.Close()) })
	return factory
}

// newKVWithUnreadableSST returns a store with the key "b" in an sstable that
// stays readable, and "c" and "d" in one that is unreadable. They don't
// overlap, so they are in the same level, and a lookup only opens the second
// one when it reaches its keys.
func newKVWithUnreadableSST(t *testing.T) KV {
	t.Helper()
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = factory.Close() })
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = kv.Close() })

	for _, keys := range [][]string{{"b"}, {"c", "d"}} {
		wb := kv.NewWriteBatch()
		for _, key := range keys {
			require.NoError(t, wb.Put(key, []byte(key)))
		}
		require.NoError(t, wb.Commit())
		require.NoError(t, wb.Close())
		require.NoError(t, kv.Flush())
	}

	// Reading the second sstable fails with an I/O error. Pebble doesn't open a
	// table it just flushed: the reads of the test are the first to open it. A
	// corrupted block wouldn't do: Pebble reports corruptions through the
	// logger's Fatalf, which exits the process.
	ssts, err := filepath.Glob(filepath.Join(kv.(*Pebble).dbPath, "*.sst"))
	require.NoError(t, err)
	require.Len(t, ssts, 2)
	require.NoError(t, os.Chmod(ssts[1], 0))
	t.Cleanup(func() { _ = os.Chmod(ssts[1], 0600) })
	if f, err := os.Open(ssts[1]); err == nil {
		assert.NoError(t, f.Close())
		t.Skip("the sstable is still readable, as when running as root")
	}
	return kv
}

// newKVWithUnreadableValueBlocks returns a store with the keys "a", "b" and
// "c" in an sstable whose value blocks cannot be read: the value of "a" sits
// with its key, and the others in the value blocks. Pebble only moves to the
// value blocks the values of the keys that share the prefix of the previous
// key: the sstable is written with a comparer that takes no key prefix, under
// the name of the default one, and without bloom filters, which index the
// prefixes.
func newKVWithUnreadableValueBlocks(t *testing.T) KV {
	t.Helper()
	dataDir := t.TempDir()
	dbPath := filepath.Join(dataDir, constant.DefaultNamespace, "shard-1")
	require.NoError(t, createMarker(dbPath, compare.EncoderNatural.Name()))
	comparer := *pebble.DefaultComparer
	comparer.Split = func([]byte) int { return 0 }
	db, err := pebble.Open(dbPath, &pebble.Options{
		Comparer:           &comparer,
		DisableWAL:         true,
		FormatMajorVersion: pebble.FormatVirtualSSTables,
	})
	require.NoError(t, err)
	for _, key := range []string{"a", "b", "c"} {
		require.NoError(t, db.Set(compare.EncoderNatural.Encode(key), []byte("value-"+key), pebble.NoSync))
	}
	require.NoError(t, db.Flush())
	require.NoError(t, db.Close())

	ssts, err := filepath.Glob(filepath.Join(dbPath, "*.sst"))
	require.NoError(t, err)
	require.Len(t, ssts, 1)
	start, end := valueBlocksRange(t, ssts[0])
	factory := newFactoryWithFaults(t, dataDir, func(op errorfs.Op) bool {
		return op.Kind == errorfs.OpFileReadAt && op.Path == ssts[0] && op.Offset >= start && op.Offset < end
	})
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
	require.NoError(t, err)
	require.Equal(t, dbPath, kv.(*Pebble).dbPath)
	t.Cleanup(func() { assert.NoError(t, kv.Close()) })
	return kv
}

// closeFaultFS passes the Close of the files and directories that it opens to
// injector, as errorfs.OpFileClose: errorfs injects no close errors. A Close
// that fails leaves the file open.
type closeFaultFS struct {
	vfs.FS
	injector errorfs.Injector
}

func (fs closeFaultFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	f, err := fs.FS.Open(name, opts...)
	if err != nil {
		return nil, err
	}
	return closeFaultFile{File: f, path: name, injector: fs.injector}, nil
}

func (fs closeFaultFS) OpenDir(name string) (vfs.File, error) {
	f, err := fs.FS.OpenDir(name)
	if err != nil {
		return nil, err
	}
	return closeFaultFile{File: f, path: name, injector: fs.injector}, nil
}

type closeFaultFile struct {
	vfs.File
	path     string
	injector errorfs.Injector
}

func (f closeFaultFile) Close() error {
	if err := f.injector.MaybeError(errorfs.Op{Kind: errorfs.OpFileClose, Path: f.path}); err != nil {
		return err
	}
	return f.File.Close()
}

var errFailpoint = errors.New("failpoint")

// newKVWithFailpoint returns a store whose failpoint name fails with
// errFailpoint, along with its factory.
func newKVWithFailpoint(t *testing.T, name string) (*PebbleFactory, KV) {
	t.Helper()
	skipWithoutFailpoints(t)
	options := NewFactoryOptionsForTest(t)
	options.Failpoints = failpoint.New(map[string]func() error{
		name: func() error { return errFailpoint },
	})
	factory, err := NewPebbleKVFactory(options)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, factory.Close()) })
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
	require.NoError(t, err)
	return factory.(*PebbleFactory), kv
}

func skipWithoutFailpoints(t *testing.T) {
	t.Helper()
	if !failpoint.Enabled {
		t.Skip("needs failpoints: disabled by the disable_trap tag")
	}
}

// snapshotChunk is a chunk of a snapshot, read ahead.
type snapshotChunk struct {
	name    string
	index   int32
	count   int32
	content []byte
}

// snapshotChunksOf returns the chunks of a snapshot of a database that holds
// the keys <prefix>-0 to <prefix>-99.
func snapshotChunksOf(t *testing.T, prefix string) []snapshotChunk {
	t.Helper()
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	defer func() { assert.NoError(t, factory.Close()) }()
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
	require.NoError(t, err)
	defer func() { assert.NoError(t, kv.Close()) }()
	putKeys(t, kv, prefix)

	snapshot, err := kv.Snapshot()
	require.NoError(t, err)
	defer func() { assert.NoError(t, snapshot.Close()) }()
	var chunks []snapshotChunk
	for ; snapshot.Valid(); snapshot.Next() {
		chunk, err := snapshot.Chunk()
		require.NoError(t, err)
		chunks = append(chunks, snapshotChunk{chunk.Name(), chunk.Index(), chunk.TotalCount(), bytes.Clone(chunk.Content())})
	}
	return chunks
}

// addChunks adds copies of the chunks: the in-memory filesystem scribbles
// over what it writes in the builds with invariants, which the race detector
// enables.
func addChunks(t *testing.T, loader SnapshotLoader, chunks []snapshotChunk) {
	t.Helper()
	for _, chunk := range chunks {
		require.NoError(t, loader.AddChunk(chunk.name, chunk.index, chunk.count, bytes.Clone(chunk.content)))
	}
}

// installSnapshot installs the chunks as the database of shard 1.
func installSnapshot(t *testing.T, factory Factory, chunks []snapshotChunk) {
	t.Helper()
	loader, err := factory.NewSnapshotLoader(context.Background(), constant.DefaultNamespace, 1, SnapshotLoaderOptions{})
	require.NoError(t, err)
	addChunks(t, loader, chunks)
	require.NoError(t, loader.Install(noCheck, nil, noLogClear))
	require.NoError(t, loader.Close())
}

func noCheck(string) error { return nil }

func noLogClear() error { return nil }

func putKeys(t *testing.T, kv KV, prefix string) {
	t.Helper()
	wb := kv.NewWriteBatch()
	for i := range 100 {
		require.NoError(t, wb.Put(fmt.Sprintf("%s-%d", prefix, i), []byte(prefix)))
	}
	require.NoError(t, wb.Commit())
	require.NoError(t, wb.Close())
}

func assertKeys(t *testing.T, kv KV, prefix string) {
	t.Helper()
	for i := range 100 {
		key := fmt.Sprintf("%s-%d", prefix, i)
		_, value, closer, err := kv.Get(key, ComparisonEqual, NoInternalKeys)
		require.NoError(t, err, key)
		assert.Equal(t, prefix, string(value))
		require.NoError(t, closer.Close())
	}
}

func assertNoKeys(t *testing.T, kv KV, prefix string) {
	t.Helper()
	_, _, _, err := kv.Get(prefix+"-0", ComparisonEqual, NoInternalKeys)
	assert.ErrorIs(t, err, errs.ErrDBKeyNotFound)
}

// assertNoInstallFiles asserts that no install of a snapshot is in progress.
func assertNoInstallFiles(t *testing.T, fs vfs.FS, paths installPaths) {
	t.Helper()
	for _, path := range []string{paths.stage, paths.marker, paths.replaced} {
		_, err := fs.Stat(path)
		assert.ErrorIs(t, err, os.ErrNotExist, path)
	}
}

// memFSWithRoom returns a filesystem in memory, which reports a disk with room
// for any snapshot.
func memFSWithRoom() *vfs.MemFS {
	fs := vfs.NewCrashableMem()
	fs.TestingSetDiskUsage(vfs.DiskUsage{TotalBytes: 1 << 40, AvailBytes: 1 << 40})
	return fs
}

func newMemFactory(t *testing.T, fs vfs.FS, failpoints *failpoint.Failpoints) *PebbleFactory {
	t.Helper()
	factory, err := NewPebbleKVFactory(&FactoryOptions{DataDir: "/data", CacheSizeMB: 1, FS: fs, Failpoints: failpoints})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, factory.Close()) })
	return factory.(*PebbleFactory)
}

func memDBOptions(fs vfs.FS) *pebble.Options {
	return &pebble.Options{
		FS:                 fs,
		DisableWAL:         true,
		Logger:             &pebbleLogger{slog.Default()},
		FormatMajorVersion: pebble.FormatVirtualSSTables,
	}
}

// writeMemDB writes a database at path on fs that holds the keys <prefix>-0
// to <prefix>-99, and syncs it.
func writeMemDB(t *testing.T, fs vfs.FS, path string, prefix string) {
	t.Helper()
	db, err := pebble.Open(path, memDBOptions(fs))
	require.NoError(t, err)
	for i := range 100 {
		require.NoError(t, db.Set(compare.EncoderNatural.Encode(fmt.Sprintf("%s-%d", prefix, i)), []byte(prefix), pebble.NoSync))
	}
	require.NoError(t, db.Flush())
	require.NoError(t, db.Close())
}

// keysOnFS returns how many of the keys "old-<i>" and "new-<i>" the database at
// path on fs holds, by prefix.
func keysOnFS(t *testing.T, fs vfs.FS, path string) map[string]int {
	t.Helper()
	options := memDBOptions(fs)
	options.ReadOnly = true
	db, err := pebble.Open(path, options)
	require.NoError(t, err)
	defer func() { assert.NoError(t, db.Close()) }()
	keys := map[string]int{}
	for _, prefix := range []string{"old", "new"} {
		for i := range 100 {
			_, closer, err := db.Get(compare.EncoderNatural.Encode(fmt.Sprintf("%s-%d", prefix, i)))
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			require.NoError(t, err)
			require.NoError(t, closer.Close())
			keys[prefix]++
		}
	}
	return keys
}

// afterOp matches the operations that target matches, once trigger matched
// one.
func afterOp(trigger, target func(op errorfs.Op) bool) func(op errorfs.Op) bool {
	var triggered atomic.Bool
	return func(op errorfs.Op) bool {
		if trigger(op) {
			triggered.Store(true)
			return false
		}
		return triggered.Load() && target(op)
	}
}
