// Copyright 2021 Matrix Origin
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

package types

import (
	"math/rand"
	"testing"
)

func BenchmarkTSCompareV1(b *testing.B) {
	total := 3000
	tsls := make([]TS, total)
	tsrs := make([]TS, total)

	for i := 0; i < total; i++ {
		tsls[i] = BuildTS(rand.Int63n(100), rand.Uint32())
		tsrs[i] = BuildTS(rand.Int63n(100), rand.Uint32())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tsls[i%total].Compare(tsrs[i%total])
	}
}

func BenchmarkTSCompareV2(b *testing.B) {
	total := 3000
	tsls := make([]TS, total)
	tsrs := make([]TS, total)

	for i := 0; i < total; i++ {
		tsls[i] = BuildTS(rand.Int63n(100), rand.Uint32())
		tsrs[i] = BuildTS(rand.Int63n(100), rand.Uint32())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tsls[i%total].CompareV2(tsrs[i%total])
	}
}
