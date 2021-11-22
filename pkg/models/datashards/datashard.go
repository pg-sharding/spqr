package datashards

import "github.com/pg-sharding/spqr/pkg/config"

type DataShard struct {
	ID string
	Cfg *config.ShardCfg

}

func NewDataShard(name string, cfg *config.ShardCfg) *DataShard {
	return &DataShard{
		ID:  name,
		Cfg: cfg,
	}
}
