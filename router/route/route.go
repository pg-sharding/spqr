package route

import (
	"sync"

	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/datashard"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
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
	beRule *config.BackendRule
	frRule *config.FrontendRule

	clPool   client.Pool
	servPool datashard.DBPool

	mu sync.Mutex
	// protects this
	cachedParams bool
	params       shard.ParameterSet
}

func NewRoute(beRule *config.BackendRule, frRule *config.FrontendRule, mapping map[string]*config.Shard) *Route {
	return &Route{
		beRule:   beRule,
		frRule:   frRule,
		servPool: datashard.NewConnPool(mapping),
		clPool:   client.NewClientPool(),
		params:   shard.ParameterSet{},
	}
}

func (r *Route) SetParams(ps shard.ParameterSet) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cachedParams = true
	r.params = ps
}

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

	serv, err := r.servPool.Connection("internal", anyK, r.beRule, "")
	if err != nil {
		spqrlog.Logger.PrintError(err)
		return shard.ParameterSet{}, err
	}

	r.cachedParams = true
	r.params = serv.Params()

	if err := r.servPool.Put(serv); err != nil {
		return nil, err
	}

	return r.params, nil
}

func (r *Route) ServPool() datashard.DBPool {
	return r.servPool
}

func (r *Route) BeRule() *config.BackendRule {
	return r.beRule
}

func (r *Route) FrRule() *config.FrontendRule {
	return r.frRule
}

func (r *Route) NofityClients(cb func(cl client.Client) error) error {
	return r.clPool.ClientPoolForeach(cb)
}

func (r *Route) AddClient(cl client.Client) error {
	return r.clPool.Put(cl)
}
func (r *Route) ReleaseClient(cl client.Client) error {
	return r.clPool.Pop(cl)
}
