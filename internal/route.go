package internal

import (
	"sync"

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

type shardKey struct {
	name string
}

type Route struct {
	beRule *config.BERule
	frRule *config.FRRule

	mu sync.Mutex

	servPoolPending map[shardKey][]Server

	client  *SpqrClient
	mapping *config.ShardMapping
}

func (r *Route) Client() *SpqrClient {
	return r.client
}

func (r *Route) Unroute(shardName string, cl *SpqrClient) error {
	key := shardKey{
		name: shardName,
	}

	r.mu.Lock()

	srv := cl.Server()
	if err := srv.Cleanup(); err != nil {
		return err
	}
	cl.Unroute()

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

func (r *Route) starttupMsgFromSh(shard *config.ShardCfg) *pgproto3.StartupMessage {

	sm := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters: map[string]string{
			"application_name": "app",
			"client_encoding":  "UTF8",
			"user":             shard.ConnUsr,
			"database":         shard.ConnUsr,
		},
	}
	return sm
}

func (r *Route) GetConn(proto string, shardName string) (Server, error) {

	key := shardKey{
		name: shardName,
	}

	var ret Server

	r.mu.Lock()

	shard := NewShard(shardName, r.mapping.SQPRShards[shardName])

	if srv, ok := r.servPoolPending[key]; ok && len(srv) > 0 {
		ret, r.servPoolPending[key] = r.servPoolPending[key][0], r.servPoolPending[key][1:]

		r.mu.Unlock()
		return ret, nil
	} else if !ok {
		r.servPoolPending[key] = make([]Server, 0)
	}

	netconn, err := shard.Connect(proto)
	if err != nil {
		return nil, err
	}

	srv := NewServer(r.beRule, netconn, shard)

	if err := shard.ReqBackendSsl(srv); err != nil {
		return nil, err
	}

	r.mu.Unlock()

	if err := srv.initConn(r.starttupMsgFromSh(shard.Cfg())); err != nil {
		return nil, err
	}

	return srv, nil
}
