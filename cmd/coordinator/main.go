package main

import (
	"github.com/pg-sharding/spqr/coordinator/app"
	"github.com/pg-sharding/spqr/coordinator/provider"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
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

		db, err := qdb.NewQDB(qdbImpl)
		if err != nil {
			return err
		}
		
		coordinator := provider.NewCoordinator(db)
		app := app.NewApp(coordinator)
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
