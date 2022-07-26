package rrouter

import (
	"crypto/tls"
	"sync"
	"time"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/router/pkg/datashard"
	"github.com/pg-sharding/spqr/router/pkg/route"
	"github.com/wal-g/tracelog"
)

type Watchdog interface {
	Watch(sh datashard.Shard)
	AddInstance(host string) error
	Run()
}

func NewShardWatchDog(tlsconfig *tls.Config, shname string, rp RoutePool) (Watchdog, error) {

	cfgs := config.RouterConfig().ShardMapping[shname].Hosts

	hostConns := make([]conn.DBInstance, 0, len(cfgs))

	for _, h := range cfgs {

		i, err := conn.NewInstanceConn(h, tlsconfig)

		if err != nil {
			return nil, err
		}

		hostConns = append(hostConns, i)
	}

	return &ShardPrimaryWatchdog{
		hostConns: hostConns,
		tlsconfig: tlsconfig,
		rp:        rp,
		shname:    shname,
	}, nil
}

type ShardPrimaryWatchdog struct {
	mu        sync.Mutex
	tlsconfig *tls.Config

	rp RoutePool

	shname string

	hostConns []conn.DBInstance
}

func (s *ShardPrimaryWatchdog) AddInstance(host string) error {
	instance, err := conn.NewInstanceConn(host, s.tlsconfig)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.hostConns = append(s.hostConns, instance)
	return nil
}

func (s *ShardPrimaryWatchdog) Run() {
	go func() {

		var prvMaster conn.DBInstance

		tracelog.InfoLogger.Printf("datashard watchdog %s started", s.shname)

		for {

			func() {

				if prvMaster != nil {
					if ok, err := prvMaster.CheckRW(); err == nil && ok {
						// nice
						return
					} else if err != nil {
						tracelog.InfoLogger.Printf("failed to check primary on %v", err)
					}
				}

				for _, host := range s.hostConns {
					if ok, err := host.CheckRW(); err == nil && ok && host.Hostname() != prvMaster.Hostname() {
						prvMaster = host
						// notify

						tracelog.InfoLogger.Printf("notifying about new master %v", host.Hostname())

						_ = s.rp.NotifyRoutes(func(route *route.Route) error {
							if err := route.ServPool().UpdateHostStatus(s.shname, prvMaster.Hostname(), false); err != nil {
								return err
							}

							if err := route.ServPool().UpdateHostStatus(s.shname, host.Hostname(), false); err != nil {
								return err
							}

							return nil
						})

						return
					} else if err != nil {
						tracelog.InfoLogger.Printf("failed to check primary on %s", host.Hostname())
					}
				}
			}()

			time.Sleep(time.Second * 10)
		}

	}()
}

func (s *ShardPrimaryWatchdog) Watch(sh datashard.Shard) {
	// add to notify queue
}

var _ Watchdog = &ShardPrimaryWatchdog{}
