package app

import (
	"context"
	"time"

	"github.com/pg-sharding/spqr/balancer"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
)

type App struct {
	balancer balancer.Balancer
}

func NewApp(b balancer.Balancer) *App {
	return &App{
		balancer: b,
	}
}

func (app *App) Run() error {
	spqrlog.Zero.Info().Msg("Running balancer")

	ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(config.BalancerConfig().TimeoutSec)*time.Second)
	defer cancel()
	app.balancer.RunBalancer(ctx)
	return nil
}
