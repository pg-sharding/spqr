package main

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/router/app"
	router "github.com/pg-sharding/spqr/router/pkg"
)

var (
	rcfgPath string
)

var rootCmd = &cobra.Command{
	Use:   "./spqr-rr run --config `path-to-config-folder`",
	Short: "sqpr-rr",
	Long:  "spqr-rr",
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
	Short: "run router",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := config.LoadRouterCfg(rcfgPath); err != nil {
			return err
		}

		// tracelog.UpdateLogLevel(tracelog.ErrorLogLevel)

		ctx, cancelCtx := context.WithCancel(context.Background())

		defer cancelCtx()

		spqr, err := router.NewRouter(ctx)
		if err != nil {
			return errors.Wrap(err, "router failed to start")
		}

		app := app.NewApp(spqr)

		wg := &sync.WaitGroup{}

		wg.Add(1)
		go func(wg *sync.WaitGroup) {

			err := app.ProcPG(ctx)
			tracelog.ErrorLogger.FatalOnError(err)
			wg.Done()
		}(wg)

		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			err := app.ServGrpc(ctx)
			tracelog.ErrorLogger.FatalOnError(err)
			wg.Done()
		}(wg)

		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			err := app.ProcADM(ctx)
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
