package datashards

import (
	"context"
)

type ShardsMgr interface {
	ListDataShards(ctx context.Context) []*DataShard
	AddDataShard(ctx context.Context, ds *DataShard) error
}

type ShardsManager interface {
	AddDataShard(ctx context.Context, shard *DataShard) error
	AddWorldShard(ctx context.Context, shard *DataShard) error
	ListShards(ctx context.Context) ([]*DataShard, error)
	GetShardInfo(ctx context.Context, shardID string) (*DataShard, error)
}
