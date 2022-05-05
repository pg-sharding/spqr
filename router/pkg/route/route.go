package route

import (
	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/router/pkg/datashard"
)

type Key struct {
	usr string
	db  string
}

func NewRouteKey(usr, db string) *Key {
	return &Key{
		usr: usr,
		db:  db,
	}
}

func (r *Key) String() string {
	return r.db + " " + r.usr
}

type Route struct {
	beRule *config.BERule
	frRule *config.FRRule

	clPool   client.Pool
	servPool datashard.DBPool
}

func NewRoute(beRule *config.BERule, frRule *config.FRRule, mapping map[string]*config.ShardCfg) *Route {
	return &Route{
		beRule:   beRule,
		frRule:   frRule,
		servPool: datashard.NewConnPool(mapping),
		clPool:   client.NewClientPool(),
	}
}

func (r *Route) Params() (datashard.ParameterSet, error) {
	var anyK kr.ShardKey
	for k := range config.RouterConfig().RulesConfig.ShardMapping {
		anyK.Name = k
		break
	}
	serv, err := r.servPool.Connection(anyK, r.beRule)
	if err != nil {
		spqrlog.Logger.PrintError(err)
		return datashard.ParameterSet{}, err
	}

	return serv.Params(), nil
}

func (r *Route) ServPool() datashard.DBPool {
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
