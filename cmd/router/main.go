package main

import (
	"sync"

	"github.com/pg-sharding/spqr/app"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/router"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
)

var (
	rcfgPath string
)

var rootCmd = &cobra.Command{
	Use:   "router run --config `path-to-data-folder`",
	Short: "SPQR",
	Long:  "Stateless Postgres Query Rrouter",
	CompletionOptions: cobra.CompletionOptions{
		DisableDefaultCmd: true,
	},
	SilenceUsage:  true,
	SilenceErrors: true,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		tracelog.ErrorLogger.Fatal(err)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&rcfgPath, "config", "c", "/etc/router/config.yaml", "path to config file")
	rootCmd.AddCommand(runCmd)
}

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "run SPQR",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := config.Load(rcfgPath); err != nil {
			return err
		}
		spqr, err := router.NewRouter()
		if err != nil {
			return errors.Wrap(err, "SPQR creation failed")
		}

		app := app.NewApp(spqr)

		wg := &sync.WaitGroup{}

		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			err := app.ProcPG()
			tracelog.ErrorLogger.FatalOnError(err)
			wg.Done()
		}(wg)

		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			err := app.ServHttp()
			tracelog.ErrorLogger.FatalOnError(err)
			wg.Done()
		}(wg)

		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			err := app.ProcADM()
			tracelog.ErrorLogger.FatalOnError(err)
			wg.Done()
		}(wg)

		wg.Wait()

		return nil
	},
}

func main() {
	Execute()
}
