package datashards

import (
	"github.com/pg-sharding/spqr/pkg/config"
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
)

type DataShard struct {
	ID  string
	Cfg *config.Shard
}

// NewDataShard creates a new DataShard instance with the given name and configuration.
//
// Parameters:
//   - name: The name of the shard.
//   - cfg: The configuration of the shard.
//
// Returns:
//   - *DataShard: The created DataShard instance.
func NewDataShard(name string, cfg *config.Shard) *DataShard {
	return &DataShard{
		ID:  name,
		Cfg: cfg,
	}
}

// DataShardToProto converts a DataShard object to a proto.Shard object.
// It takes a pointer to a DataShard as input and returns a pointer to a proto.Shard.
//
// Parameters:
//   - shard: The DataShard object to convert.
//
// Returns:
//   - *proto.Shard: The converted proto.Shard object.
func DataShardToProto(shard *DataShard) *proto.Shard {
	return &proto.Shard{
		Hosts: shard.Cfg.Hosts,
		Id:    shard.ID,
	}
}

// DataShardFromProto creates a new DataShard instance from the given proto.Shard.
// It initializes the DataShard with the shard ID and hosts from the proto.Shard,
// and sets the shard type to config.DataShard.
//
// Parameters:
//   - shard: The proto.Shard object to convert.
//
// Returns:
//   - *DataShard: The created DataShard instance.
func DataShardFromProto(shard *proto.Shard) *DataShard {
	return NewDataShard(shard.Id, &config.Shard{
		Hosts: shard.Hosts,
		Type:  config.DataShard,
	})
}

func DataShardFromDb(shard *qdb.Shard) *DataShard {
	return NewDataShard(shard.ID, &config.Shard{
		Hosts: shard.Hosts,
		Type:  config.DataShard,
	})
}
