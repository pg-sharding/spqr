package datashards

import (
	"github.com/pg-sharding/spqr/pkg/config"
	proto "github.com/pg-sharding/spqr/pkg/protos"
)

type DataShard struct {
	ID  string
	Cfg *config.Shard
}

func NewDataShard(name string, cfg *config.Shard) *DataShard {
	return &DataShard{
		ID:  name,
		Cfg: cfg,
	}
}

func DataShardFromProto(reply *proto.ShardInfo) *DataShard {
	cfg := &config.Shard{
		Hosts: reply.Hosts,
		Type:  "",
		TLS:   nil,
	}
	return NewDataShard(reply.Id, cfg)
}
