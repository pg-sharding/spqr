package route

import (
	"sync"

	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/startup"
	"github.com/pg-sharding/spqr/pkg/tsa"
)

type Key struct {
	user string
	db   string
}

func (r *Key) Usr() string {
	return r.user
}

func (r *Key) DB() string {
	return r.db
}

func NewRouteKey(user, db string) *Key {
	return &Key{
		user: user,
		db:   db,
	}
}

func (r *Key) String() string {
	return r.db + " " + r.user
}

type Route struct {
	pool.PoolIterator

	beRule *config.BackendRule
	frRule *config.FrontendRule

	clPool   client.Pool
	servPool pool.MultiShardTSAPool

	mu sync.Mutex
	// protects this
	cachedParams bool
	params       shard.ParameterSet
}

func NewRoute(beRule *config.BackendRule, frRule *config.FrontendRule, mapping map[string]*config.Shard) *Route {
	sp := &startup.StartupParams{}
	if frRule != nil {
		sp.SearchPath = frRule.SearchPath
	}

	var preferAZ string
	if config.RouterConfig().PreferSameAvailabilityZone {
		preferAZ = config.RouterConfig().AvailabilityZone
	}

	route := &Route{
		beRule:   beRule,
		frRule:   frRule,
		servPool: pool.NewDBPool(mapping, sp, preferAZ),
		clPool:   client.NewClientPool(),
		params:   shard.ParameterSet{},
	}
	route.servPool.SetRule(beRule)
	return route
}

// TODO : unit tests
func (r *Route) SetParams(ps shard.ParameterSet) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cachedParams = true
	r.params = ps
}

// TODO : unit tests
func (r *Route) Params() (shard.ParameterSet, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.cachedParams {
		return r.params, nil
	}

	var anyK kr.ShardKey
	for k := range r.servPool.ShardMapping() {
		anyK.Name = k
		break
	}

	// maxuint64
	serv, err := r.servPool.ConnectionWithTSA(0xFFFFFFFFFFFFFFFF, anyK, tsa.TSA(config.TargetSessionAttrsAny))
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return shard.ParameterSet{}, err
	}

	r.cachedParams = true
	r.params = serv.Params()

	if err := r.servPool.Put(serv); err != nil {
		return nil, err
	}

	return r.params, nil
}

func (r *Route) ServPool() pool.MultiShardTSAPool {
	return r.servPool
}

func (r *Route) BeRule() *config.BackendRule {
	return r.beRule
}

func (r *Route) FrRule() *config.FrontendRule {
	return r.frRule
}

func (r *Route) NofityClients(cb func(cl client.ClientInfo) error) error {
	return r.clPool.ClientPoolForeach(cb)
}

func (r *Route) AddClient(cl client.Client) error {
	return r.clPool.Put(cl)
}

func (r *Route) ReleaseClient(clientID uint) (bool, error) {
	return r.clPool.Pop(clientID)
}

func (r *Route) ForEachPool(cb func(pool.Pool) error) error {
	return r.servPool.ForEachPool(cb)
}
