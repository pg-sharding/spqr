package pkg

import (
	"context"

	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/router/pkg/console"
	"github.com/wal-g/tracelog"
)

type Executer struct {
	cfg config.ExecuterCfg
}

func NewExecuter(cfg config.ExecuterCfg) *Executer {
	return &Executer{
		cfg: cfg,
	}
}

func (e *Executer) SPIexec(ctx context.Context, console console.Console, cl client.Client, cmds []string) error {
	for _, cmd := range cmds {
		tracelog.InfoLogger.Printf("executing init sql cmd %s", cmd)
		if err := console.ProcessQuery(ctx, cmd, cl); err != nil {
			tracelog.InfoLogger.PrintError(err)
		}
	}

	return nil
}
