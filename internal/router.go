package internal

import (
	"crypto/tls"
	"net"
	"sync"

	"github.com/jackc/pgproto3"
	"github.com/pg-sharding/spqr/internal/config"
	"github.com/pkg/errors"
)



type Router struct {
	CFG       config.RouterConfig
	mu        sync.Mutex
	routePool map[routeKey][]*Route

	mpFrontendRules map[routeKey]*config.FRRule

	cfg *tls.Config
}

func NewRouter(cfg config.RouterConfig) (*Router, error) {

	mp := make(map[routeKey]*config.FRRule)

	for _, e := range cfg.FrontendRules {
		//tracelog.InfoLogger.Printf("frontend rule for %v %v: auth method %v\n", e.DB, e.Usr, e.AuthRule.Am)
		mp[routeKey{
			usr: e.Usr,
			db:  e.DB,
		}] = e
	}

	router := &Router{
		CFG:             cfg,
		mu:              sync.Mutex{},
		routePool:       map[routeKey][]*Route{},
		mpFrontendRules: mp,
	}

	cert, err := tls.LoadX509KeyPair(cfg.TLSCfg.CertFile, cfg.TLSCfg.KeyFile)
	if err != nil {
		return nil, errors.Wrap(err, "failed to load frontend tls conf")
	}
	router.cfg = &tls.Config{Certificates: []tls.Certificate{cert}, InsecureSkipVerify: true}

	return router, nil
}

func (r *Router) PreRoute(conn net.Conn) (*SpqrClient, error) {

	cl := NewClient(conn)

	if err := cl.Init(r.cfg, r.CFG.ReqSSL); err != nil {
		return nil, err
	}

	//tracelog.InfoLogger.Printf("routing %v %v", cl.Usr(), cl.DB())

	// match client frontend rule
	key := routeKey{
		usr: cl.Usr(),
		db:  cl.DB(),
	}

	var frRule *config.FRRule
	frRule, ok := r.mpFrontendRules[key]
	if !ok {

		for _, msg := range []pgproto3.BackendMessage{
			&pgproto3.ErrorResponse{
				Message: "failed to route",
			},
		} {
			if err := cl.Send(msg); err != nil {
				return nil, errors.Wrap(err, "failed to make route failure resp")
			}
		}

		return nil, errors.New("Failed to route client")
	}

	cl.AssignRule(frRule)

	if err := cl.Auth(); err != nil {
		return nil, err
	}

	var route *Route

	r.mu.Lock()
	{
		if routes, ok := r.routePool[key]; ok && len(routes) > 0 {
			route, routes = routes[0], routes[1:]

			r.routePool[key] = routes
		} else {
			if !ok {
				r.routePool[key] = make([]*Route, 0)
			}

			route = NewRoute(r.CFG.BackendRules, frRule)

			r.routePool[key] = append(r.routePool[key], route)
		}
	}
	r.mu.Unlock()

	cl.AssignRoute(route)

	return cl, nil
}

func (r *Router) ListShards() []string {
	var ret []string

	for _, sh := range r.CFG.BackendRules {
		ret = append(ret, sh.SHStorage.ConnAddr)
	}

	return ret
}

func Connect(proto string, rule *config.BERule) (net.Conn, error) {
	//tracelog.InfoLogger.Printf("acquire backend connection on addr %v\n", rule.SHStorage.ConnAddr)

	return net.Dial(proto, rule.SHStorage.ConnAddr)
}
