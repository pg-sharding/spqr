package core

import (
	"sync"

	"github.com/jackc/pgproto3"
)

type routeKey struct {
	usr string
	db  string
}
type shardKey struct {
	i int
}

type Route struct {
	rule []*BERule

	mu sync.Mutex

	servPoolPending map[shardKey][]*ShServer

	client *ShClient

	//serv   ShServer
}

func (r *Route) Client() *ShClient {
	return r.client
}

func (r *Route) Unroute(i int, cl *ShClient) {
	key := shardKey{
		i: i,
	}

	r.mu.Lock()

	srv := cl.ShardConn()
	cl.Unroute()
	r.servPoolPending[key] = append(r.servPoolPending[key], srv)

	r.mu.Unlock()
}

func NewRoute(rules []*BERule) *Route {
	return &Route{
		rule:            rules,
		servPoolPending: map[shardKey][]*ShServer{},
		mu:              sync.Mutex{},
	}
}

func (r *Route) smFromSh(i int) *pgproto3.StartupMessage {

	sm := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters: map[string]string{
			"application_name": "shgo",
			"client_encoding":  "UTF8",
			"user":             r.rule[i].SHStorage.ConnUsr,
			"database":         r.rule[i].SHStorage.ConnDB,
		},
	}
	return sm
}

func (r *Route) GetConn(proto string, indx int) (*ShServer, error) {

	key := shardKey{
		indx,
	}

	var ret *ShServer

	r.mu.Lock()

	if srv, ok := r.servPoolPending[key]; ok && len(srv) > 0 {
		ret, r.servPoolPending[key] = r.servPoolPending[key][0], r.servPoolPending[key][1:]

		r.mu.Unlock()
		return ret, nil
	} else if !ok {
		r.servPoolPending[key] = make([]*ShServer, 0)
	}

	netconn, err := Connect(proto, r.rule[indx])
	if err != nil {
		return nil, err
	}

	srv := NewServer(r.rule[indx], netconn)
	if r.rule[indx].SHStorage.ReqSSL {
		if err := srv.ReqBackendSsl(); err != nil {
			return nil, err
		}
	}

	r.mu.Unlock()

	if err := srv.initConn(r.smFromSh(indx)); err != nil {
		return nil, err
	}

	return srv, nil
}
