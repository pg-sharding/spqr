package app

import (
	"context"
	balancerPkg "github.com/pg-sharding/spqr/balancer/pkg"
	"github.com/pg-sharding/spqr/pkg/config"
	"golang.yandex/hasql"
	"strconv"
)

type App struct {
	coordinator *balancerPkg.Coordinator
	database *balancerPkg.Database
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
		port, err := strconv.Atoi(shard.Port)
		if err != nil {
			return nil, err
		}
		shardClusters[id], err = balancerPkg.NewCluster(
			shard.Hosts,
			cfg.InstallationDBName,
			cfg.InstallationTableName,
			cfg.InstallationPassword,
			"",
			"",
			port)
		if err != nil {
			return nil, err
		}
	}
	installation := balancerPkg.Installation{}

	err = installation.Init(
		cfg.InstallationDBName,
		cfg.InstallationTableName,
		cfg.InstallationUserName,
		cfg.InstallationPassword,
		&shardClusters,
		cfg.InstallationMaxRetries,
		)
	if err != nil {
		return nil, err
	}

	db := balancerPkg.Database{}
	err = db.Init(cfg.DatabaseHosts, cfg.DatabaseMaxRetries, cfg.InstallationDBName, cfg.InstallationTableName, cfg.DatabasePassword)
	if err != nil {
		return nil, err
	}

	balancer.Init(&installation, &coordinator, &db)

	return &App{
		coordinator: &coordinator,
		installation: &installation,
		database: &db,
		balancer: balancer,
	}, nil
}

func (app *App) ProcBalancer(ctx context.Context) error {

	//TODO return error
	app.balancer.BrutForceStrategy()
	return nil
}
