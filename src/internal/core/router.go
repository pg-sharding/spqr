package core

import (
	"github.com/wal-g/tracelog"
	"golang.org/x/xerrors"
	"net"
	"sync"
)

type RouterConfig struct {
	BackendRules    []*BERule            `json:"backend_rules" toml:"backend_rules" yaml:"backend_rules"`
	FrontendRules   map[routeKey]*FRRule `json:"frontend_rules" toml:"frontend_rules" yaml:"frontend_rules"`
	MaxConnPerRoute int                  `json:"max_conn_per_route" toml:"max_conn_per_route" yaml:"max_conn_per_route"`
	ReqSSL          bool                 `json:"require_ssl" toml:"require_ssl" yaml:"require_ssl"`

	PROTO string `json:"proto" toml:"proto" yaml:"proto"`
}

type Router struct {
	CFG       RouterConfig
	mu        sync.Mutex
	routePool map[routeKey][]*Route
}

func NewRouter(cfg RouterConfig) *Router {
	return &Router{
		CFG:       cfg,
		mu:        sync.Mutex{},
		routePool: map[routeKey][]*Route{},
	}
}

func (r *Router) PreRoute(conn net.Conn) (*ShClient, error) {

	cl := NewClient(conn)

	if err := cl.Init(r.CFG.ReqSSL); err != nil {
		return nil, err
	}

	// match client frontend rule
	key := routeKey{
		usr: cl.Usr(),
		db:  cl.DB(),
	}

	var frRule *FRRule
	frRule, ok := r.CFG.FrontendRules[key]
	if !ok {
		return nil, xerrors.Errorf(
			"failed ro match route for user %v database %v", key.usr, key.db)
	}

	cl.AssignRule(frRule)

	if err := cl.Auth(); err != nil {
		return nil, err
	}

	r.mu.Lock()
	{
		if routes, ok := r.routePool[key]; ok && len(routes) > 0 {

			route, routes := routes[0], routes[1:]

			r.routePool[key] = routes

			cl.AssignRoute(route)

		} else {
			if !ok {
				r.routePool[key] = make([]*Route, 0)
			}

			route := NewRoute(r.CFG.BackendRules, frRule)

			r.routePool[key] = append(r.routePool[key], route)

			cl.AssignRoute(route)
		}
	}
	r.mu.Unlock()
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
	tracelog.InfoLogger.Printf("acquire backend connection on addr %v\n", rule.SHStorage.ConnAddr)

	return net.Dial(proto, rule.SHStorage.ConnAddr)
}
