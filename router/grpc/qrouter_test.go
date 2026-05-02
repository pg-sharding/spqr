package grpc

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/emptypb"

	mocksh "github.com/pg-sharding/spqr/pkg/mock/shard"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	mockrr "github.com/pg-sharding/spqr/router/mock/rulerouter"
)

func TestListBackendConnections_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRR := mockrr.NewMockRuleRouter(ctrl)

	mockShard1 := mocksh.NewMockShardHostCtl(ctrl)
	mockShard2 := mocksh.NewMockShardHostCtl(ctrl)

	mockShard1.EXPECT().ID().Return(uint(1)).AnyTimes()
	mockShard1.EXPECT().InstanceHostname().Return("host1.local").AnyTimes()
	mockShard1.EXPECT().ShardKeyName().Return("key1").AnyTimes()
	mockShard1.EXPECT().Usr().Return("testuser1").AnyTimes()
	mockShard1.EXPECT().DB().Return("testdb1").AnyTimes()
	mockShard1.EXPECT().Sync().Return(int64(0)).AnyTimes()
	mockShard1.EXPECT().TxServed().Return(int64(0)).AnyTimes()
	mockShard1.EXPECT().TxStatus().Return(txstatus.TXIDLE).AnyTimes()

	mockShard2.EXPECT().ID().Return(uint(2)).AnyTimes()
	mockShard2.EXPECT().InstanceHostname().Return("host2.local").AnyTimes()
	mockShard2.EXPECT().ShardKeyName().Return("key2").AnyTimes()
	mockShard2.EXPECT().Usr().Return("testuser2").AnyTimes()
	mockShard2.EXPECT().DB().Return("testdb2").AnyTimes()
	mockShard2.EXPECT().Sync().Return(int64(0)).AnyTimes()
	mockShard2.EXPECT().TxServed().Return(int64(0)).AnyTimes()
	mockShard2.EXPECT().TxStatus().Return(txstatus.TXIDLE).AnyTimes()

	testShards := []shard.ShardHostCtl{mockShard1, mockShard2}

	mockRR.EXPECT().
		ForEach(gomock.Any()).
		DoAndReturn(func(fn func(shard.ShardHostCtl) error) error {
			for _, sh := range testShards {
				if err := fn(sh); err != nil {
					return err
				}
			}
			return nil
		}).
		Times(1)

	server := &LocalQrouterServer{rr: mockRR}
	reply, err := server.ListBackendConnections(context.Background(), &emptypb.Empty{})

	assert.NoError(t, err)
	assert.Len(t, reply.Conns, 2)

	assert.Equal(t, uint64(1), reply.Conns[0].BackendConnectionId)
	assert.Equal(t, "host1.local", reply.Conns[0].Hostname)
	assert.Equal(t, "key1", reply.Conns[0].ShardKeyName)
}

func TestListBackendConnections_Empty(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRR := mockrr.NewMockRuleRouter(ctrl)

	mockRR.EXPECT().
		ForEach(gomock.Any()).
		DoAndReturn(func(fn func(shard.ShardHostCtl) error) error {
			return nil
		}).
		Times(1)

	server := &LocalQrouterServer{rr: mockRR}
	reply, err := server.ListBackendConnections(context.Background(), &emptypb.Empty{})

	assert.NoError(t, err)
	assert.Empty(t, reply.Conns)
}

func TestListBackendConnections_ForEachError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRR := mockrr.NewMockRuleRouter(ctrl)
	testErr := errors.New("router not initialized")

	mockRR.EXPECT().
		ForEach(gomock.Any()).
		Return(testErr).
		Times(1)

	server := &LocalQrouterServer{rr: mockRR}
	reply, err := server.ListBackendConnections(context.Background(), &emptypb.Empty{})

	assert.ErrorIs(t, err, testErr)
	assert.Empty(t, reply.Conns)
}
