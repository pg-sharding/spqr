package app

import (
	"context"
	"github.com/pg-sharding/spqr/balancer"
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
	spqrlog.Zero.Info().Msg("running coordinator app")

	app.balancer.RunBalancer(context.TODO())
	return nil
}
