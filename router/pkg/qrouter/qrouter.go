package qrouter

import (
	"github.com/pg-sharding/spqr/coordinator/qdb/qdb"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/kr"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
	"github.com/pkg/errors"
)

const NOSHARD = ""

type ShardRoute struct {
	Shkey     kr.ShardKey
	Matchedkr kr.KeyRange
}

type Qrouter interface {
	Route(q string) []ShardRoute

	AddShardingColumn(col string) error
	AddLocalTable(tname string) error

	AddKeyRange(kr kr.KeyRange) error
	Shards() []string
	KeyRanges() []kr.KeyRange

	AddShard(name string, cfg *config.ShardCfg) error

	Lock(krid string) error
	UnLock(krid string) error
	Split(req *spqrparser.SplitKeyRange) error
	Unite(req *spqrparser.UniteKeyRange) error

	Subscribe(krid string, krst qdb.KeyRangeStatus, noitfyio chan<- interface{}) error
}

func NewQrouter(qtype config.QrouterType) (Qrouter, error) {
	switch qtype {
	case config.ShardQrouter:
		return NewShardQrouter(config.Get().QRouterCfg.LocalShard)
	case config.LocalQrouter:
		return NewLocalQrouter(config.Get().QRouterCfg.LocalShard)
	case config.ProxyQrouter:
		return NewProxyRouter()
	default:
		return nil, errors.Errorf("unknown qrouter type %v", config.Get().QRouterCfg.Qtype)
	}

}
