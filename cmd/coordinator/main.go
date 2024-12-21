package main

import (
	"fmt"
	"log"
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
		cfgStr, err := config.LoadCoordinatorCfg(cfgPath)
		if err != nil {
			return err
		}
		log.Println("Running config:", cfgStr)

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

		coordinator, err := provider.NewCoordinator(frTLS, db)
		if err != nil {
			return err
		}

		app := app.NewApp(coordinator)
		return app.Run(true)
	},
}

var testCmd = &cobra.Command{
	Use:   "test-config {path-to-config | -c path-to-config}",
	Short: "Load, validate and print the given config file",
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) > 0 {
			cfgPath = args[0]
		}
		cfgStr, err := config.LoadCoordinatorCfg(cfgPath)
		if err != nil {
			return err
		}
		fmt.Println(cfgStr)
		return nil
	},
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&cfgPath, "config", "c", "/etc/spqr/coordinator.yaml", "path to config file")
	rootCmd.PersistentFlags().StringVarP(&qdbImpl, "qdb-impl", "", "etcd", "which implementation of QDB to use.")
	rootCmd.PersistentFlags().IntVarP(&gomaxprocs, "gomaxprocs", "", 0, "GOMAXPROCS value")

	rootCmd.AddCommand(testCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
	}
}
