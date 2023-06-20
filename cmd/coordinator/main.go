package main

import (
	"github.com/pg-sharding/spqr/coordinator/app"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/spf13/cobra"
)

var cfgPath string
var qdbImpl string

var rootCmd = &cobra.Command{
	Use: "spqr-coordinator --config `path-to-config`",
	CompletionOptions: cobra.CompletionOptions{
		DisableDefaultCmd: true,
	},
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := config.LoadCoordinatorCfg(cfgPath); err != nil {
			return err
		}

		app, err := app.NewApp(qdbImpl)
		if err != nil {
			return err
		}

		return app.Run()
	},
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&cfgPath, "config", "c", "/etc/spqr/coordinator.yaml", "path to config file")
	rootCmd.PersistentFlags().StringVarP(&qdbImpl, "qdb-impl", "", "etcd", "which implementation of QDB to use.")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		spqrlog.Logger.FatalOnError(err)
	}
}
