package datashards

import (
	"context"
)

type ShardsMgr interface {
	AddDataShard(ctx context.Context, shard *DataShard) error
	AddWorldShard(ctx context.Context, shard *DataShard) error
	ListShards(ctx context.Context) ([]*DataShard, error)
	GetShardInfo(ctx context.Context, shardID string) (*DataShard, error)
	DropShard(ctx context.Context, id string) error
}
