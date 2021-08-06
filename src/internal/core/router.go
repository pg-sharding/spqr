package core

import (
	"golang.org/x/xerrors"
	"net"
	"sync"
)

type Router struct {
	mu sync.Mutex
	routePool map[routeKey][]*Route
	rules map[routeKey]*Rule
}

func (r * Router) PreRoute(conn net.Conn) (*Route, error) {

	cl := NewClient(conn)

	if err := cl.Init(); err != nil {
		return nil, err
	}

	r.mu.Lock()
	{
		key := routeKey{
			usr: cl.Usr(),
			db: cl.DB(),
		}
		if routes, ok := r.routePool[key]; ok && len(routes) > 0 {

			route, routes := routes[0], routes[1:]

			r.routePool[key] = routes

			route.assignCLient(cl)

			r.mu.Unlock()
			return route, nil
		} else {
			if !ok {
				r.routePool[key] = make([]*Route, 0)
			}


			if rule, ok := r.rules[key]; !ok {
				r.mu.Unlock()
				return nil, xerrors.Errorf("failed to match route")
			} else{
				route := NewRoute(rule)

				r.routePool[key] = append(r.routePool[key], route)

				route.assignCLient(cl)

				r.mu.Unlock()
				return route, nil
			}
		}
	}

	return nil, nil
}


//
//func (router *Router) Connect(i int) (*core.ShServer, error) {
//
//	shConf := conn.cfg.ShardMapping[i]
//
//	conn.connMu[i].Lock()
//	{
//		if connl := conn.connMp[i]; len(connl) > 0 {
//			serv := connl[0]
//			connl = connl[1:]
//			conn.connMp[i] = connl
//		} else {
//
//
//
//			serv := core.NewServer(
//			)
//		}
//	}
//	conn.connMu[i].Unlock()
//	return serv, nil
//}