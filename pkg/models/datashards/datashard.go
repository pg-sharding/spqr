package datashards

import "github.com/pg-sharding/spqr/pkg/config"

type DataShard struct {
	ID  string
	Cfg *config.ShardCfg
}

func NewDataShard(name string, cfg *config.ShardCfg) *DataShard {
	return &DataShard{
		ID:  name,
		Cfg: cfg,
	}
}

// Coordinator structs
// TODO: unify all structs: from coordinator, balancer, and router.
type Shard struct {
	ID   string
	Addr string
}

func NewShard(ID string, addr string) *Shard {
	return &Shard{
		ID:   ID,
		Addr: addr,
	}
}

type ShardInfo struct {
	ID    string
	Hosts []string
	Port  string
}
