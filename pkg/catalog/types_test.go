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

package catalog

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/require"
)

func TestObjectLocationMarshalAndUnmarshal(t *testing.T) {
	var loc ObjectLocation
	for i := 0; i < len(loc); i++ {
		loc[i] = byte(i)
	}

	data, err := loc.Marshal()
	require.NoError(t, err)

	var ret ObjectLocation
	err = ret.Unmarshal(data)
	require.NoError(t, err)
	require.Equal(t, loc, ret)
}

var globalLocation objectio.Location

// BenchmarkBlockInfoMetaLocation checks heap allocation for BlockInfo.MetaLocation.
// go test -run None -bench BenchmarkBlockInfoMetaLocation -benchmem
func BenchmarkBlockInfoMetaLocation(b *testing.B) {
	info := BlockInfo{}
	b.ResetTimer()
	var ret objectio.Location
	for i := 0; i < b.N; i++ {
		ret = info.MetaLocation()
	}
	globalLocation = ret
}

// BenchmarkObjectLocationToLocation checks heap allocation for ObjectLocation.ToLocation.
// go test -run None -bench BenchmarkObjectLocationToLocation -benchmem
func BenchmarkObjectLocationToLocation(b *testing.B) {
	loc := ObjectLocation{}
	b.ResetTimer()
	var ret objectio.Location
	for i := 0; i < b.N; i++ {
		ret = loc.ToLocation()
	}
	globalLocation = ret
}
