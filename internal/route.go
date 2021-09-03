package internal

import (
	"github.com/jackc/pgproto3"
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

func NewRoute(rule *config.BERule, frRules *config.FRRule, mapping map[string]*config.ShardCfg) *Route {
	return &Route{
		beRule:   rule,
		frRule:   frRules,
		servPool: NewShardPool(mapping),
	}
}

func (r *Route) startupMsgFromSh(shard *config.ShardCfg) *pgproto3.StartupMessage {

	sm := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters: map[string]string{
			"application_name": "app",
			"client_encoding":  "UTF8",
			"user":             shard.ConnUsr,
			"database":         shard.ConnDB,
		},
	}
	return sm
}
