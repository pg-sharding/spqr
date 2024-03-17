package shard

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestShardIDs(t *testing.T) {
	assert := assert.New(t)

	emptyShards := []Shard{}
	assert.Equal([]uint{}, ShardIDs(emptyShards), "ShardIDs should return an empty slice for no shards")

	oneShard := []Shard{&mockShard{id: 1}}
	assert.Equal([]uint{1}, ShardIDs(oneShard), "ShardIDs should return a slice with one ID for one shard")

	multipleShards := []Shard{&mockShard{id: 1}, &mockShard{id: 2}, &mockShard{id: 3}}
	assert.Equal([]uint{1, 2, 3}, ShardIDs(multipleShards), "ShardIDs should return a slice with multiple IDs for multiple shards")
}
