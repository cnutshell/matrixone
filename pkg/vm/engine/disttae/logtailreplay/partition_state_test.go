// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logtailreplay

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

var (
	globalObjectNameShort *objectio.ObjectNameShort
	globalBool            bool
)

// BenchmarkObjectEntry checks heap allocation for ObjectEntry.
// $ go test -run None -bench BenchmarkObjectEntry -benchmem
func BenchmarkObjectEntry(b *testing.B) {
	location := [objectio.LocationLen]byte{1, 2, 3, 4, 5, 6, 7, 8}
	obj := ObjectEntry{
		metaLocation: location,
	}

	b.Run("ObjectShortName", func(b *testing.B) {
		var localObjectNameShort *objectio.ObjectNameShort
		for i := 0; i < b.N; i++ {
			localObjectNameShort = obj.ObjectShortName()
		}
		globalObjectNameShort = localObjectNameShort
	})

	b.Run("Less", func(b *testing.B) {
		var localBool bool
		for i := 0; i < b.N; i++ {
			localBool = obj.Less(obj)
		}
		globalBool = localBool
	})
}

// BenchmarkBlockEntry checks heap allocation for BlockEntry.Less.
// $ go test -run None -bench BenchmarkBlockEntry -benchmem
func BenchmarkBlockEntry(b *testing.B) {
	blkLhs := BlockEntry{
		BlockInfo: catalog.BlockInfo{
			MetaLoc: [objectio.LocationLen]byte{1, 2, 3, 4, 5, 6, 7, 8},
		},
	}
	blkRhs := BlockEntry{
		BlockInfo: catalog.BlockInfo{
			MetaLoc: [objectio.LocationLen]byte{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
	}

	b.Run("BlockEntry.Less", func(b *testing.B) {
		var localBool bool
		for i := 0; i < b.N; i++ {
			localBool = blkLhs.Less(blkRhs)
		}
		globalBool = localBool
	})
}

func BenchmarkPartitionState(b *testing.B) {
	partition := NewPartition()
	end := make(chan struct{})
	defer func() {
		close(end)
	}()

	// concurrent writer
	go func() {
		for {
			select {
			case <-end:
				return
			default:
			}
			state, end := partition.MutateState()
			_ = state
			end()
		}
	}()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			state := partition.state.Load()
			iter := state.NewRowsIter(types.BuildTS(0, 0), nil, false)
			iter.Close()
		}
	})

}
