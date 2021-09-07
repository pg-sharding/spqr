package rrouter

import (
	"crypto/tls"
	"github.com/pg-sharding/spqr/internal/config"
	"github.com/pg-sharding/spqr/internal/conn"
	"github.com/wal-g/tracelog"
	"time"
)

type Watchdog interface {
	Watch(sh Shard)
}


func NewShardWatchDog(cfgs []*config.InstanceCFG, tlscfg *tls.Config, sslmode string) (Watchdog, error) {

	hostConns := make([]conn.DBInstance, 0, len(cfgs))

	for _, h := range cfgs {

		i, err := conn.NewInstanceConn(h, tlscfg, sslmode)

		if err != nil {
			return nil, err
		}

		hostConns = append(hostConns, i)
	}

	return &ShardPrimaryWatchdog{
		hostConns: hostConns,
	}, nil
}

type ShardPrimaryWatchdog struct {
	hostConns []conn.DBInstance
}

func (s *ShardPrimaryWatchdog) Run () {
	go func() {

		var prvMaster conn.DBInstance

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

						break
					} else if err != nil {
						tracelog.InfoLogger.Printf("failed to check primary on %s", host.Hostname())
					}
				}
			}()

			time.Sleep(time.Second * 10)
		}

	}()
}

func (s *ShardPrimaryWatchdog) Watch(sh Shard) {
	// add to notify queue
}

var _ Watchdog = &ShardPrimaryWatchdog{}

