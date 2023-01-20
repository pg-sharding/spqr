package main

import (
	"github.com/spf13/cobra"

	"github.com/pg-sharding/spqr/coordinator/app"
	"github.com/pg-sharding/spqr/coordinator/provider"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
)

var cfgPath string
var qdbImpl string

var qdbImplEtcd = "etcd"
var qdbImplMem = "mem"

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

		var db qdb.QDB
		var err error

		switch qdbImpl {
		case qdbImplEtcd:
			db, err = qdb.NewEtcdQDB(config.CoordinatorConfig().QdbAddr)
			if err != nil {
				spqrlog.Logger.FatalOnError(err)
				// exit
			}
		case qdbImplMem:
			db, err = qdb.NewMemQDB()
			if err != nil {
				spqrlog.Logger.FatalOnError(err)
				// exit
			}
		default:
			spqrlog.Logger.Fatalf("qdb implementation %s is invalid", qdbImpl)
		}

		coordinator := provider.NewCoordinator(db)

		app := app.NewApp(coordinator)

		err = app.Run()
		spqrlog.Logger.PrintError(err)

		return err
	},
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&cfgPath, "config", "c", "/etc/spqr/coordinator.yaml", "path to config file")
	rootCmd.PersistentFlags().StringVarP(&qdbImpl, "qdb-impl", "", qdbImplEtcd, "which implementation of QDB to use.")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		spqrlog.Logger.FatalOnError(err)
	}
}

func main() {
	Execute()
}
