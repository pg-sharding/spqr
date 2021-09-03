package internal

import (
	"github.com/pg-sharding/spqr/internal/config"
)

type routeKey struct {
	usr string
	db  string
}

func (r *routeKey) String() string {
	return r.db + " " + r.usr
}

type ShardKey struct {
	name string
}

func NewSHKey(name string) ShardKey {
	return ShardKey{
		name: name,
	}
}

type Route struct {
	beRule *config.BERule
	frRule *config.FRRule

	clPool   ClientPool
	servPool ShardPool
}

func NewRoute(beRule *config.BERule, frRule *config.FRRule, mapping map[string]*config.ShardCfg) *Route {
	return &Route{
		beRule:   beRule,
		frRule:   frRule,
		servPool: NewShardPool(mapping),
	}
}


func (r *Route) AddClient(cl Client) error {
	return r.clPool.Put(cl)
}
