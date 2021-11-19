package route

import (
	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/conn"
)

type RouteKey struct {
	usr string
	db  string
}

func NewRouteKey(usr, db string) *RouteKey {
	return &RouteKey{
		usr: usr,
		db:  db,
	}
}

func (r *RouteKey) String() string {
	return r.db + " " + r.usr
}

type Route struct {
	beRule *config.BERule
	frRule *config.FRRule

	clPool   client.Pool
	servPool conn.ConnPool
}

func NewRoute(beRule *config.BERule, frRule *config.FRRule, mapping map[string]*config.ShardCfg) *Route {
	return &Route{
		beRule:   beRule,
		frRule:   frRule,
		servPool: conn.NewConnPool(mapping),
		clPool:   client.NewClientPool(),
	}
}

func (r *Route) ServPool() conn.ConnPool {
	return r.servPool
}

func (r *Route) BeRule() *config.BERule {
	return r.beRule
}

func (r *Route) FrRule() *config.FRRule {
	return r.frRule
}

func (r *Route) NofityClients(cb func(cl client.Client) error) error {
	return r.clPool.ClientPoolForeach(cb)
}

func (r *Route) AddClient(cl client.Client) error {
	return r.clPool.Put(cl)
}
