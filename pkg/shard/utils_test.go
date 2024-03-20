package shard_test

import (
	"github.com/golang/mock/gomock"
	mocksh "github.com/pg-sharding/spqr/pkg/mock/shard"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestShardIDs(t *testing.T) {
	assert := assert.New(t)

	ctrl := gomock.NewController(t)

	sh1 := mocksh.NewMockShard(ctrl)
	sh1.EXPECT().ID().Return(uint(1)).AnyTimes()

	sh2 := mocksh.NewMockShard(ctrl)
	sh2.EXPECT().ID().Return(uint(2)).AnyTimes()

	sh3 := mocksh.NewMockShard(ctrl)
	sh3.EXPECT().ID().Return(uint(3)).AnyTimes()

	var emptyShards []shard.Shard
	assert.Equal([]uint{}, shard.ShardIDs(emptyShards), "ShardIDs should return an empty slice for no shards")

	oneShard := []shard.Shard{sh1}
	assert.Equal([]uint{1}, shard.ShardIDs(oneShard), "ShardIDs should return a slice with one ID for one shard")

	multipleShards := []shard.Shard{sh1, sh2, sh3}
	assert.Equal([]uint{1, 2, 3}, shard.ShardIDs(multipleShards), "ShardIDs should return a slice with multiple IDs for multiple shards")
}
