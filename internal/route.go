package internal

import (
	"sync"

	"github.com/jackc/pgproto3"
	"github.com/pg-sharding/spqr/internal/config"
	"github.com/wal-g/tracelog"
)

type routeKey struct {
	usr string
	db  string
}

func (r *routeKey) String() string {
	return r.db + " " + r.usr
}

type shardKey struct {
	name string
}

type Route struct {
	beRule *config.BERule
	frRule *config.FRRule

	mu sync.Mutex

	servPoolPending map[shardKey][]Server

	client  Client
	mapping map[string]*config.ShardCfg
}

func (r *Route) Client() Client {
	return r.client
}

func (r *Route) Unroute(shardName string, cl Client) error {
	key := shardKey{
		name: shardName,
	}

	r.mu.Lock()

	srv := cl.Server()
	if srv != nil {
		if err := srv.Cleanup(); err != nil {
			return err
		}
		cl.Unroute()
	}

	r.servPoolPending[key] = append(r.servPoolPending[key], srv)

	r.mu.Unlock()

	return nil
}

func NewRoute(rule *config.BERule, frRules *config.FRRule) *Route {
	return &Route{
		beRule:          rule,
		frRule:          frRules,
		servPoolPending: map[shardKey][]Server{},
		mu:              sync.Mutex{},
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

func (r *Route) GetConn(proto string, shard Shard) (Server, error) {

	key := shardKey{
		name: shard.Name(),
	}

	var ret Server

	r.mu.Lock()
	if srv, ok := r.servPoolPending[key]; ok && len(srv) > 0 {
		ret, r.servPoolPending[key] = r.servPoolPending[key][0], r.servPoolPending[key][1:]

		r.mu.Unlock()
		return ret, nil
	} else if !ok {
		r.servPoolPending[key] = make([]Server, 0)
	}

	netconn, err := shard.Connect(proto)
	if err != nil {
		tracelog.ErrorLogger.PrintError(err)
		return nil, err
	}

	srv := NewShardServer(r.beRule, netconn, shard)
	tracelog.InfoLogger.Printf("acquiring backend connection to shard %s", shard.Name())

	if shard.Cfg().TLSCfg.ReqSSL {
		if err := srv.pgconn.ReqBackendSsl(shard.Cfg().TLSConfig); err != nil {
			return nil, err
		}
	}

	r.mu.Unlock()

	if err := srv.pgconn.initConn(r.startupMsgFromSh(shard.Cfg())); err != nil {
		return nil, err
	}

	return srv, nil
}
