package app

import (
	"context"
	"github.com/pg-sharding/spqr/balancer/pkg"
)

type App struct {
	balancer *pkg.Balancer
}

func NewApp(balancer *pkg.Balancer) *App {
	return &App{
		balancer: balancer,
	}
}

func (app *App) GetShardsConnStrings(ctx context.Context) (map[int]string, error) {
	return nil, nil
}

//TODO

func (app *App) ProcBalancer(ctx context.Context) error {
	return nil
}
