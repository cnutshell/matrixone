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

package client

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/stretchr/testify/assert"
)

func TestAdjustClient(t *testing.T) {
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	c := &txnClient{}
	c.adjust()
	assert.NotNil(t, c.generator)
	assert.NotNil(t, c.generator)
}

func TestNewTxn(t *testing.T) {
	rt := runtime.NewRuntime(metadata.ServiceType_CN, "",
		logutil.GetPanicLogger(),
		runtime.WithClock(clock.NewHLCClock(func() int64 {
			return 1
		}, 0)))
	runtime.SetupProcessLevelRuntime(rt)
	c := NewTxnClient(newTestTxnSender())
	if tc, ok := c.(TxnClientWithFeature); ok {
		tc.Resume()
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	tx, err := c.New(ctx, newTestTimestamp(0))
	assert.Nil(t, err)
	txnMeta := tx.(*txnOperator).mu.txn
	assert.Equal(t, timestamp.Timestamp{PhysicalTime: 1}, txnMeta.SnapshotTS)
	assert.NotEmpty(t, txnMeta.ID)
	assert.Equal(t, txn.TxnStatus_Active, txnMeta.Status)
}

func TestNewTxnWithSnapshotTS(t *testing.T) {
	rt := runtime.NewRuntime(metadata.ServiceType_CN, "",
		logutil.GetPanicLogger(),
		runtime.WithClock(clock.NewHLCClock(func() int64 {
			return 1
		}, 0)))
	runtime.SetupProcessLevelRuntime(rt)
	c := NewTxnClient(newTestTxnSender())
	if tc, ok := c.(TxnClientWithFeature); ok {
		tc.Resume()
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	tx, err := c.New(ctx, newTestTimestamp(0), WithSnapshotTS(timestamp.Timestamp{PhysicalTime: 10}))
	assert.Nil(t, err)
	txnMeta := tx.(*txnOperator).mu.txn
	assert.Equal(t, timestamp.Timestamp{PhysicalTime: 10}, txnMeta.SnapshotTS)
	assert.NotEmpty(t, txnMeta.ID)
	assert.Equal(t, txn.TxnStatus_Active, txnMeta.Status)
}

func TestTxnClientAbortAllRunningTxn(t *testing.T) {
	rt := runtime.NewRuntime(metadata.ServiceType_CN, "",
		logutil.GetPanicLogger(),
		runtime.WithClock(clock.NewHLCClock(func() int64 {
			return 1
		}, 0)))
	runtime.SetupProcessLevelRuntime(rt)

	c := NewTxnClient(newTestTxnSender())
	if tc, ok := c.(TxnClientWithFeature); ok {
		tc.Resume()
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	for i := 0; i < 10; i++ {
		_, err := c.New(ctx, newTestTimestamp(0))
		assert.Nil(t, err)
	}
	require.Equal(t, 10, len(c.(*txnClient).mu.txns))

	c.AbortAllRunningTxn()
	txnList := c.(*txnClient).mu.txns
	for i := range txnList {
		require.Equal(t, txn.TxnStatus_Aborted, txnList[i].Status)
	}
}

func TestTxnClientPauseAndResume(t *testing.T) {
	rt := runtime.NewRuntime(metadata.ServiceType_CN, "",
		logutil.GetPanicLogger(),
		runtime.WithClock(clock.NewHLCClock(func() int64 {
			return 1
		}, 0)))
	runtime.SetupProcessLevelRuntime(rt)
	c := NewTxnClient(newTestTxnSender())

	tcFeature, ok1 := c.(TxnClientWithFeature)
	tcClient, ok2 := c.(*txnClient)
	require.Equal(t, true, ok1 && ok2)
	tcFeature.Pause()
	require.Equal(t, paused, tcClient.mu.state)
	tcFeature.Resume()
	require.Equal(t, normal, tcClient.mu.state)
}
