package main

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"

	"github.com/pg-sharding/spqr/coordinator/app"
	"github.com/pg-sharding/spqr/coordinator/provider"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/qdb/etcdqdb"
)

var cfgPath string

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

		db, err := etcdqdb.NewEtcdQDB(config.CoordinatorConfig().QdbAddr)
		if err != nil {
			tracelog.ErrorLogger.FatalError(err)
			// exit
		}
		coordinator := provider.NewCoordinator(db)

		app := app.NewApp(coordinator)

		err = app.Run()
		tracelog.ErrorLogger.PrintError(err)

		return err
	},
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&cfgPath, "config", "c", "/etc/spqr-coordinator/config.yaml", "path to config file")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		tracelog.ErrorLogger.Fatal(err)
	}
}

func main() {
	Execute()
}
