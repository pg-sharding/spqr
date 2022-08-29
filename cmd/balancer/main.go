package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/spf13/cobra"

	"github.com/pg-sharding/spqr/balancer/app"
	"github.com/pg-sharding/spqr/balancer/pkg"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
)

var (
	cfgPath  string
	rcfgPath string
)

var rootCmd = &cobra.Command{
	Use: "spqr-balancer --config `path-to-config`",
	CompletionOptions: cobra.CompletionOptions{
		DisableDefaultCmd: true,
	},
	SilenceUsage:  false,
	SilenceErrors: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := config.LoadBalancerCfg(cfgPath); err != nil {
			return err
		}

		ctx, cancelCtx := context.WithCancel(context.Background())

		defer cancelCtx()
		// init db-,coordinator- and installation-class
		balancer := pkg.Balancer{}
		bCfg := *config.BalancerConfig()

		app, err := app.NewApp(&balancer, bCfg)
		if err != nil {
			return fmt.Errorf("error while creating balancer app: %s", err)
		}

		err = app.ProcBalancer(ctx)
		if err != nil {
			spqrlog.Logger.PrintError(err)
		}

		if err := config.LoadRouterCfg(rcfgPath); err != nil {
			return err
		}

		wg := &sync.WaitGroup{}

		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			err := app.ServeAdminConsole(ctx, bCfg.TLS)
			spqrlog.Logger.FatalOnError(err)
			wg.Done()
		}(wg)

		wg.Wait()

		return err
	},
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&cfgPath, "config", "c", "/etc/spqr/balancer.yml", "path to config file")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		spqrlog.Logger.FatalOnError(err)
	}
}

func main() {
	Execute()
}
