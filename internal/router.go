package internal

import (
	"crypto/tls"
	"log"
	"net"
	"os"
	"sync"

	"github.com/jackc/pgproto3"
	"github.com/pg-sharding/spqr/internal/config"
	"github.com/pg-sharding/spqr/internal/qrouter"
	"github.com/pkg/errors"
)

type Router struct {
	Cfg       config.RouterConfig
	ConsoleDB Console
	mu        sync.Mutex
	routePool map[routeKey][]*Route

	frontendRules map[routeKey]*config.FRRule
	backendRules  map[routeKey]*config.BERule

	cfg *tls.Config
	lg  *log.Logger
}

func (r *Router) ServeConsole(netconn net.Conn) error {
	return r.ConsoleDB.Serve(netconn)
}

func NewRouter(cfg config.RouterConfig, qrouter qrouter.Qrouter) (*Router, error) {

	frs := make(map[routeKey]*config.FRRule)

	for _, e := range cfg.FrontendRules {
		frs[routeKey{
			usr: e.RK.Usr,
			db:  e.RK.DB,
		}] = e
	}
	bes := make(map[routeKey]*config.BERule)

	for _, e := range cfg.BackendRules {
		bes[routeKey{
			usr: e.RK.Usr,
			db:  e.RK.DB,
		}] = e
	}

	router := &Router{
		Cfg:           cfg,
		mu:            sync.Mutex{},
		routePool:     map[routeKey][]*Route{},
		frontendRules: frs,
		backendRules:  bes,
		lg:            log.New(os.Stdout, "router", 0),
	}

	cert, err := tls.LoadX509KeyPair(cfg.TLSCfg.CertFile, cfg.TLSCfg.KeyFile)
	router.lg.Printf("loading tls cert file %s, key file %s", cfg.TLSCfg.CertFile, cfg.TLSCfg.KeyFile)
	if err != nil {
		return nil, errors.Wrap(err, "failed to load frontend tls conf")
	}
	router.cfg = &tls.Config{Certificates: []tls.Certificate{cert}, InsecureSkipVerify: true}

	consoleDB := NewConsole(router.cfg, qrouter)

	router.ConsoleDB = consoleDB

	return router, nil
}

func (r *Router) PreRoute(conn net.Conn) (*SpqrClient, error) {

	cl := NewClient(conn)

	if err := cl.Init(r.cfg, r.Cfg.TLSCfg.ReqSSL); err != nil {
		return nil, err
	}

	// match client frontend rule
	key := routeKey{
		usr: cl.Usr(),
		db:  cl.DB(),
	}

	frRule, ok := r.frontendRules[key]
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

	beRule, ok := r.backendRules[key]
	if !ok {
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

			route = NewRoute(beRule, frRule)

			r.routePool[key] = append(r.routePool[key], route)
		}
	}
	r.mu.Unlock()

	cl.AssignRoute(route)

	return cl, nil
}

func (r *Router) ListShards() []string {
	var ret []string

	for _, sh := range r.Cfg.ShardMapping {
		ret = append(ret, sh.ConnAddr)
	}

	return ret
}
