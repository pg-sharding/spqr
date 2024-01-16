package main

import (
	"github.com/pg-sharding/spqr/coordinator/app"
	"github.com/pg-sharding/spqr/coordinator/provider"
	"github.com/pg-sharding/spqr/pkg"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/spf13/cobra"
	"runtime"
)

var (
	cfgPath    string
	qdbImpl    string
	gomaxprocs int
)

var rootCmd = &cobra.Command{
	Use:   "spqr-coordinator run --config `path-to-config`",
	Short: "sqpr-coordinator",
	Long:  "spqr-coordinator",
	CompletionOptions: cobra.CompletionOptions{
		DisableDefaultCmd: true,
	},
	Version:       pkg.SpqrVersionRevision,
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := config.LoadCoordinatorCfg(cfgPath); err != nil {
			return err
		}

		if gomaxprocs > 0 {
			runtime.GOMAXPROCS(gomaxprocs)
		}

		db, err := qdb.NewXQDB(qdbImpl)
		if err != nil {
			return err
		}

		coordinator := provider.NewCoordinator(db)
		app := app.NewApp(coordinator)
		return app.Run(true)
	},
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&cfgPath, "config", "c", "/etc/spqr/coordinator.yaml", "path to config file")
	rootCmd.PersistentFlags().StringVarP(&qdbImpl, "qdb-impl", "", "etcd", "which implementation of QDB to use.")
	rootCmd.PersistentFlags().IntVarP(&gomaxprocs, "gomaxprocs", "", 0, "GOMAXPROCS value")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		spqrlog.Zero.Error().Err(err)
	}
}
