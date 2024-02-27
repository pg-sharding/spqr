package main

import (
	"fmt"
	"runtime"

	"github.com/pg-sharding/spqr/coordinator/app"
	"github.com/pg-sharding/spqr/coordinator/provider"
	"github.com/pg-sharding/spqr/pkg"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/spf13/cobra"
)

var (
	cfgPath    string
	qdbImpl    string
	gomaxprocs int
)

var rootCmd = &cobra.Command{
	Use:   "spqr-coordinator run --config `path-to-config`",
	Short: "spqr-coordinator",
	Long:  "spqr-coordinator",
	CompletionOptions: cobra.CompletionOptions{
		DisableDefaultCmd: true,
	},
	Version:       pkg.SpqrVersionRevision,
	SilenceUsage:  false,
	SilenceErrors: false,
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

		// frontend
		frTLS, err := config.CoordinatorConfig().FrontendTLS.Init(config.CoordinatorConfig().Host)
		if err != nil {
			return fmt.Errorf("init frontend TLS: %w", err)
		}

		coordinator := provider.NewCoordinator(frTLS, db)
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
		spqrlog.Zero.Error().Err(err).Msg("")
	}
}
