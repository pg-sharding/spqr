package main

import (
	"context"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime/pprof"
	"sync"
	"syscall"

	"github.com/pg-sharding/spqr/pkg/spqrlog"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/router/app"
	router "github.com/pg-sharding/spqr/router/pkg"
)

var (
	rcfgPath    string
	saveProfie  bool
	profileFile string
)

var rootCmd = &cobra.Command{
	Use:   "./spqr-router run --config `path-to-config-folder`",
	Short: "sqpr-rr",
	Long:  "spqr-router",
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
	rootCmd.PersistentFlags().StringVarP(&rcfgPath, "config", "c", "/etc/spqr/router.yaml", "path to config file")
	rootCmd.PersistentFlags().StringVarP(&profileFile, "profile-file", "p", "/etc/spqr/router.prof", "path to profile file")
	rootCmd.PersistentFlags().BoolVar(&saveProfie, "profile", false, "path to config file")
	rootCmd.AddCommand(runCmd)
}

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "run router",
	RunE: func(cmd *cobra.Command, args []string) error {

		var pprofFile *os.File
		var err error
		if saveProfie {
			pprofFile, err = os.Create(profileFile)
			if err != nil {
				return err
			}
			spqrlog.Logger.Printf(spqrlog.LOG, "starting cpu prof with %s", profileFile)
			if err := pprof.StartCPUProfile(pprofFile); err != nil {
				return err
			}
		}

		rcfg, err := config.LoadRouterCfg(rcfgPath)
		if err != nil {
			return err
		}

		ctx, cancelCtx := context.WithCancel(context.Background())
		defer cancelCtx()

		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR1)

		router, err := router.NewRouter(ctx, &rcfg)
		if err != nil {
			return errors.Wrap(err, "router failed to start")
		}

		app := app.NewApp(router)

		go func() {
			for {
				s := <-sigs
				spqrlog.Logger.Printf(spqrlog.LOG, "received signal %v", s)

				switch s {
				case syscall.SIGUSR1:
					// write profile
					pprof.StopCPUProfile()

					if err := pprofFile.Close(); err != nil {
						spqrlog.Logger.PrintError(err)
					}
					return
				case syscall.SIGHUP:
					// reread config file
					err := router.RuleRouter.Reload(rcfgPath)
					if err != nil {
						spqrlog.Logger.PrintError(err)
					}
				case syscall.SIGINT, syscall.SIGTERM:
					cancelCtx()
					return
				default:
					return
				}
			}
		}()

		wg := &sync.WaitGroup{}

		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			err := app.ServeRouter(ctx)
			if err != nil {
				spqrlog.Logger.PrintError(err)
			}
			wg.Done()
		}(wg)

		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			err := app.ServeGrpcApi(ctx)
			if err != nil {
				spqrlog.Logger.PrintError(err)
			}
			wg.Done()
		}(wg)

		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			err := app.ServeAdminConsole(ctx)
			if err != nil {
				spqrlog.Logger.PrintError(err)
			}
			wg.Done()
		}(wg)

		wg.Wait()

		return nil
	},
}

func main() {
	Execute()
}
