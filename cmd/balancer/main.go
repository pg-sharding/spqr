package main

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"

	"github.com/pg-sharding/spqr/balancer/app"
	"github.com/pg-sharding/spqr/balancer/pkg"
	"github.com/pg-sharding/spqr/pkg/config"
)

var cfgPath string

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

		app, err := app.NewApp(&balancer, *config.BalancerConfig())
		if err != nil {
			return fmt.Errorf("error while creating balancer app: %s", err)
		}

		err = app.ProcBalancer(ctx)
		tracelog.ErrorLogger.PrintError(err)

		return err
	},
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&cfgPath, "config", "c", "/etc/balancer/config.yaml", "path to config file")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		tracelog.ErrorLogger.Fatal(err)
	}
}

func main() {
	Execute()
}
