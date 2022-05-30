package app

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"golang.yandex/hasql"

	balancerPkg "github.com/pg-sharding/spqr/balancer/pkg"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
)

type App struct {
	coordinator  *balancerPkg.Coordinator
	database     *balancerPkg.Database
	installation *balancerPkg.Installation

	balancer *balancerPkg.Balancer
}

func NewApp(balancer *balancerPkg.Balancer, cfg config.BalancerCfg) (*App, error) {
	coordinator := balancerPkg.Coordinator{}
	err := coordinator.Init(cfg.CoordinatorAddress, 3)
	if err != nil {
		return nil, err
	}
	shards, err := coordinator.ShardsList()
	if err != nil {
		return nil, err
	}
	shardClusters := map[int]*hasql.Cluster{}

	for id, shard := range *shards {
		if shard.Port == "" {
			_, shard.Port, err = net.SplitHostPort(shard.Hosts[0])
			if err != nil {
				return nil, err
			}
		}
		port, err := strconv.Atoi(shard.Port)
		if err != nil {
			return nil, err
		}
		shardClusters[id], err = balancerPkg.NewCluster(
			shard.Hosts,
			cfg.InstallationDBName,
			cfg.InstallationUserName,
			cfg.InstallationPassword,
			cfg.InstallationSSLMode,
			"",
			port)
		if err != nil {
			return nil, fmt.Errorf("failed to create a new cluster connection: %v", err)
		}
	}
	installation := balancerPkg.Installation{}

	err = installation.Init(
		cfg.InstallationDBName,
		cfg.InstallationTableName,
		cfg.InstallationShardingKey,
		cfg.InstallationUserName,
		cfg.InstallationPassword,
		&shardClusters,
		cfg.InstallationMaxRetries,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to init balancer: %v", err)
	}

	db := balancerPkg.Database{}
	err = db.Init(cfg.DatabaseHosts, cfg.DatabaseMaxRetries, cfg.InstallationDBName, cfg.InstallationTableName, cfg.DatabasePassword)
	if err != nil {
		return nil, err
	}

	balancer.Init(&installation, &coordinator, &coordinator, &db)

	return &App{
		coordinator:  &coordinator,
		installation: &installation,
		database:     &db,
		balancer:     balancer,
	}, nil
}

func (app *App) ProcBalancer(ctx context.Context) error {

	//TODO return error
	app.balancer.BrutForceStrategy()
	return nil
}

func (app *App) ProcADM(ctx context.Context, frTlsCfg config.TLSConfig) error {
	frTLS, err := frTlsCfg.Init()
	if err != nil {
		return fmt.Errorf("init frontend TLS: %w", err)
	}

	proto, admaddr := config.RouterConfig().Proto, config.RouterConfig().ADMAddr

	listener, err := net.Listen(proto, admaddr)
	if err != nil {
		return err
	}
	defer listener.Close()

	spqrlog.Logger.Printf(spqrlog.INFO, "ProcADM listening %s by %s", admaddr, proto)
	return app.balancer.RunAdm(ctx, listener, frTLS)
}
