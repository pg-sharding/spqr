package server

import (
	"testing"

	mockshard "github.com/pg-sharding/spqr/pkg/mock/shard"
	"github.com/pg-sharding/spqr/pkg/shard"
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
