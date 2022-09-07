package app

import (
	"context"
	"fmt"
	"net"

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

func NewApp(balancer *balancerPkg.Balancer, cfg config.Balancer) (*App, error) {
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
		shardClusters[id], err = balancerPkg.NewCluster(
			shard.Hosts,
			cfg.InstallationDBName,
			cfg.InstallationUserName,
			cfg.InstallationPassword,
			cfg.InstallationSSLMode,
			"")
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

func (app *App) ServeAdminConsole(ctx context.Context, clientTLSConfig config.TLSConfig) error {
	clientTLS, err := clientTLSConfig.Init()
	if err != nil {
		return fmt.Errorf("init frontend TLS: %w", err)
	}

	address := net.JoinHostPort(config.BalancerConfig().Host, config.BalancerConfig().AdminConsolePort)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	defer listener.Close()

	spqrlog.Logger.Printf(spqrlog.INFO, "SPQR balancer Admin Console is ready on %s", address)
	return app.balancer.RunAdm(ctx, listener, clientTLS)
}
