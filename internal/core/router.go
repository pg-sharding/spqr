package core

import (
	"crypto/tls"
	"net"
	"sync"

	"github.com/jackc/pgproto3"
	"github.com/wal-g/tracelog"
	"golang.org/x/xerrors"
)

type RouterConfig struct {
	BackendRules    []*BERule `json:"backend_rules" toml:"backend_rules" yaml:"backend_rules"`
	FrontendRules   []*FRRule `json:"frontend_rules" toml:"frontend_rules" yaml:"frontend_rules"`
	MaxConnPerRoute int       `json:"max_conn_per_route" toml:"max_conn_per_route" yaml:"max_conn_per_route"`
	ReqSSL          bool      `json:"require_ssl" toml:"require_ssl" yaml:"require_ssl"`

	// frontend tls settings
	TLSCfg TLSConfig `json:"tls" yaml:"tls" toml:"tls"`

	PROTO string `json:"proto" toml:"proto" yaml:"proto"`
}

type Router struct {
	CFG       RouterConfig
	mu        sync.Mutex
	routePool map[routeKey][]*Route

	mpFrontendRules map[routeKey]*FRRule

	cfg *tls.Config
}

func NewRouter(cfg RouterConfig) (*Router, error) {

	mp := make(map[routeKey]*FRRule, 0)

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

	cert, err := tls.LoadX509KeyPair(cfg.TLSCfg.TLSSertPath, cfg.TLSCfg.ServPath)
	if err != nil {
		tracelog.InfoLogger.Printf("failed to load frontend tls conf: %w", err)
		return nil, err
	}

	tlscfg := &tls.Config{Certificates: []tls.Certificate{cert}, InsecureSkipVerify: true}

	router.cfg = tlscfg

	return router, nil
}

func (r *Router) PreRoute(conn net.Conn) (*ShClient, error) {

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

	var frRule *FRRule
	frRule, ok := r.mpFrontendRules[key]
	if !ok {

		for _, msg := range []pgproto3.BackendMessage{
			&pgproto3.ErrorResponse{
				Message: "failed to route",
			},
		} {
			if err :=
				cl.Send(msg); err != nil {
				//tracelog.InfoLogger.Printf("failed to make route failure resp")
				return nil, err
			}
		}

		return nil, xerrors.Errorf("failed to route cl")
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

func Connect(proto string, rule *BERule) (net.Conn, error) {
	//tracelog.InfoLogger.Printf("acquire backend connection on addr %v\n", rule.SHStorage.ConnAddr)

	return net.Dial(proto, rule.SHStorage.ConnAddr)
}
