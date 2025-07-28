package qrouter

import (
	"context"
	"math/rand"
	"sync"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/router/cache"
	"github.com/pg-sharding/spqr/router/planner"
	"go.uber.org/atomic"
)

type ProxyQrouter struct {
	mu sync.Mutex

	ColumnMapping map[string]struct{}
	LocalTables   map[string]struct{}

	// shards
	DataShardCfgs  map[string]*config.Shard
	WorldShardCfgs map[string]*config.Shard

	cfg *config.QRouter

	mgr          meta.EntityMgr
	schemaCache  *cache.SchemaCache
	idRangeCache planner.IdentityRouterCache

	initialized *atomic.Bool
	query       *string
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

func (qr *ProxyQrouter) SchemaCache() *cache.SchemaCache {
	return qr.schemaCache
}

func (qr *ProxyQrouter) SetQuery(q *string) {
	qr.query = q
}
func (qr *ProxyQrouter) Query() *string {
	return qr.query
}

// TODO : unit tests
func (qr *ProxyQrouter) DataShardsRoutes() []kr.ShardKey {
	rc, _ := qr.mgr.ListShards(context.TODO())
	rv := make([]kr.ShardKey, 0, len(rc))
	for _, el := range rc {
		rv = append(rv, kr.ShardKey{
			Name: el.ID,
			RO:   false,
		})
	}
	return rv
}

// TODO : unit tests
func (qr *ProxyQrouter) WorldShardsRoutes() []kr.ShardKey {
	qr.mu.Lock()
	defer qr.mu.Unlock()

	var ret []kr.ShardKey

	for name := range qr.WorldShardCfgs {
		ret = append(ret, kr.ShardKey{
			Name: name,
			RO:   false,
		})
	}

	// a sort of round robin

	rand.Shuffle(len(ret), func(i, j int) {
		ret[i], ret[j] = ret[j], ret[i]
	})
	return ret
}

func NewProxyRouter(shardMapping map[string]*config.Shard,
	mgr meta.EntityMgr,
	qcfg *config.QRouter,
	cache *cache.SchemaCache,
	idRangeCache planner.IdentityRouterCache,
) (*ProxyQrouter, error) {
	proxy := &ProxyQrouter{
		WorldShardCfgs: map[string]*config.Shard{},
		initialized:    atomic.NewBool(false),
		cfg:            qcfg,
		mgr:            mgr,
		schemaCache:    cache,
		idRangeCache:   idRangeCache,
	}

	ctx := context.TODO()

	/* XXX: since memqdb is persistent on disk, in some cases, we need to recreate the whole topology.
	* TODO: get this information from the coordinator, not the config.
	 */
	sds, err := mgr.ListShards(ctx)
	if err != nil {
		return nil, err
	}
	for _, ds := range sds {
		err := mgr.DropShard(ctx, ds.ID)
		if err != nil {
			return nil, err
		}
	}

	for name, shardCfg := range shardMapping {
		switch shardCfg.Type {
		case config.WorldShard:
		case config.DataShard:
			fallthrough // default is datashard
		default:
			if err := mgr.AddDataShard(ctx, &topology.DataShard{
				ID:  name,
				Cfg: shardCfg,
			}); err != nil {
				return nil, err
			}
		}
	}
	return proxy, nil
}
