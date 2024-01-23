package qrouter

import (
	"context"
	"math/rand"
	"sync"

	"go.uber.org/atomic"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/datashards"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/router/routingstate"
)

type ProxyQrouter struct {
	mu sync.Mutex

	ColumnMapping map[string]struct{}
	LocalTables   map[string]struct{}

	// shards
	DataShardCfgs  map[string]*config.Shard
	WorldShardCfgs map[string]*config.Shard

	cfg *config.QRouter

	mgr meta.EntityMgr

	initialized *atomic.Bool
}

var _ QueryRouter = &ProxyQrouter{}

func (qr *ProxyQrouter) Initialized() bool {
	return qr.initialized.Load()
}

func (qr *ProxyQrouter) Initialize() bool {
	return qr.initialized.Swap(true)
}

func (qr *ProxyQrouter) Mgr() meta.EntityMgr {
	return qr.mgr
}

// TODO : unit tests
func (qr *ProxyQrouter) DataShardsRoutes() []*routingstate.DataShardRoute {
	rc, _ := qr.mgr.ListShards(context.TODO())
	rv := make([]*routingstate.DataShardRoute, 0, len(rc))
	for _, el := range rc {
		rv = append(rv, &routingstate.DataShardRoute{Shkey: kr.ShardKey{
			Name: el.ID,
			RW:   false,
		}})
	}
	return rv
}

// TODO : unit tests
func (qr *ProxyQrouter) WorldShardsRoutes() []*routingstate.DataShardRoute {
	qr.mu.Lock()
	defer qr.mu.Unlock()

	var ret []*routingstate.DataShardRoute

	for name := range qr.WorldShardCfgs {
		ret = append(ret, &routingstate.DataShardRoute{
			Shkey: kr.ShardKey{
				Name: name,
				RW:   true,
			},
		})
	}

	// a sort of round robin

	rand.Shuffle(len(ret), func(i, j int) {
		ret[i], ret[j] = ret[j], ret[i]
	})
	return ret
}

func NewProxyRouter(shardMapping map[string]*config.Shard, mgr meta.EntityMgr, qcfg *config.QRouter) (*ProxyQrouter, error) {
	proxy := &ProxyQrouter{
		WorldShardCfgs: map[string]*config.Shard{},
		initialized:    atomic.NewBool(false),
		cfg:            qcfg,
		mgr:            mgr,
	}

	for name, shardCfg := range shardMapping {
		switch shardCfg.Type {
		case config.WorldShard:
		case config.DataShard:
			fallthrough // default is datashard
		default:
			if err := mgr.AddDataShard(context.TODO(), &datashards.DataShard{
				ID:  name,
				Cfg: shardCfg,
			}); err != nil {
				return nil, err
			}
		}
	}
	return proxy, nil
}
