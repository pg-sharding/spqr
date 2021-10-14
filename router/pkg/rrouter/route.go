package rrouter

import (
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/qdb/qdb"
	"github.com/pg-sharding/spqr/router/pkg/conn"
)

type routeKey struct {
	usr string
	db  string
}

func (r *routeKey) String() string {
	return r.db + " " + r.usr
}

func NewSHKey(name string) qdb.ShardKey {
	return qdb.ShardKey{
		Name: name,
	}
}

type Route struct {
	beRule *config.BERule
	frRule *config.FRRule

	clPool   ClientPool
	servPool conn.ConnPool
}

func NewRoute(beRule *config.BERule, frRule *config.FRRule, mapping map[string]*config.ShardCfg) *Route {
	return &Route{
		beRule:   beRule,
		frRule:   frRule,
		servPool: conn.NewConnPool(mapping),
		clPool:   NewClientPool(),
	}
}

func (r *Route) NofityClients(cb func(cl Client) error) error {
	return r.clPool.ClientPoolForeach(cb)
}

func (r *Route) AddClient(cl Client) error {
	return r.clPool.Put(cl)
}
