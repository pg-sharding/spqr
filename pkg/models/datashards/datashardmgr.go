package datashards

import "context"

type ShardsMgr interface {
	ListDataShards(ctx context.Context) []*DataShard
	AddDataShard(ctx context.Context, ds *DataShard) error
}
