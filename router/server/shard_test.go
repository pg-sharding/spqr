package server

import (
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	mockpool "github.com/pg-sharding/spqr/pkg/mock/pool"
	mockshard "github.com/pg-sharding/spqr/pkg/mock/shard"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestName_NilShard(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srv := NewShardServer(nil)

	assert.Equal(t, "", srv.Name())
}

func TestName_WithShard(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSh := mockshard.NewMockShardHostInstance(ctrl)
	mockSh.EXPECT().Name().Return("sh1")

	srv := NewShardServer(nil)
	var sh shard.ShardHostInstance = mockSh
	srv.shard.Store(&sh)

	assert.Equal(t, "sh1", srv.Name())
}

func TestExpandGangDeployTxFailPutsUnusedConn(t *testing.T) {
	assert := assert.New(t)
	ctrl := gomock.NewController(t)

	p := mockpool.NewMockConnectionProvider(ctrl)
	active := mockshard.NewMockShardHostInstance(ctrl)
	active.EXPECT().SHKey().Return(kr.ShardKey{Name: "sh0"})
	active.EXPECT().TxStatus().Return(txstatus.TXACT)

	sh := mockshard.NewMockShardHostInstance(ctrl)
	sh.EXPECT().ID().Return(uint(1)).AnyTimes()
	sh.EXPECT().Send(&pgproto3.Query{String: "BEGIN"}).Return(nil)
	sh.EXPECT().Receive().Return(&pgproto3.ErrorResponse{Message: "begin failed"}, nil)

	p.EXPECT().ConnectionWithTSA(gomock.Any(), gomock.Any()).Return(sh, nil)
	p.EXPECT().Put(sh).Return(nil).Times(1)

	ms := NewMultiShardServerFromShard(p, active)
	assert.Error(ms.ExpandGang(pool.ConnAllocParams{}, kr.ShardKey{Name: "sh1"}, true))
	assert.Equal([]shard.ShardHostInstance{active}, ms.Datashards())
}
