package router

import (
	"bufio"
	"log"
	"os"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/router/pkg/console"
	"github.com/pg-sharding/spqr/router/pkg/rrouter"
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

func (e *Executer) ReadCmds() []string {

	f, err := os.Open(e.cfg.InitSQLPath)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	ret := make([]string, 0)

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		ret = append(ret, scanner.Text())
	}

	return ret
}

func (e *Executer) SPIexec(console console.Console, cl rrouter.Client) error {
	for _, cmd := range e.ReadCmds() {
		tracelog.InfoLogger.Printf("executing init sql cmd %s", cmd)
		if err := console.ProcessQuery(cmd, cl); err != nil {
			tracelog.InfoLogger.PrintError(err)
		}
	}

	return nil
}
