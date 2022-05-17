package main

import (
	"context"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime/pprof"
	"sync"
	"syscall"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/router/app"
	router "github.com/pg-sharding/spqr/router/pkg"
)

var (
	rcfgPath    string
	doProfie    bool
	profileFile string
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
	rootCmd.PersistentFlags().StringVarP(&profileFile, "profile-file", "p", "/etc/router/router.prof", "path to profile file")
	rootCmd.PersistentFlags().BoolVar(&doProfie, "profile", false, "path to config file")
	rootCmd.AddCommand(runCmd)
}

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "run router",
	RunE: func(cmd *cobra.Command, args []string) error {

		var pprofFile *os.File
		var err error
		if doProfie {
			pprofFile, err = os.Create(profileFile)
			if err != nil {
				return err
			}
			spqrlog.Logger.Printf(spqrlog.LOG, "starting cpu prof with %s", profileFile)
			if err := pprof.StartCPUProfile(pprofFile); err != nil {
				return err
			}
		}

		if err := config.LoadRouterCfg(rcfgPath); err != nil {
			return err
		}

		// tracelog.UpdateLogLevel(tracelog.ErrorLogLevel)

		ctx, cancelCtx := context.WithCancel(context.Background())

		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGHUP, syscall.SIGKILL, syscall.SIGTERM, syscall.SIGUSR1)

		go func() {
			defer cancelCtx()
			for {
				s := <-sigs
				spqrlog.Logger.Printf(spqrlog.LOG, "got signal %v", s)

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
				case syscall.SIGKILL, syscall.SIGTERM:
					cancelCtx()
					return
				default:
					return
				}
			}
		}()

		spqr, err := router.NewRouter(ctx)
		if err != nil {
			return errors.Wrap(err, "router failed to start")
		}

		app := app.NewApp(spqr)

		wg := &sync.WaitGroup{}

		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			err := app.ProcPG(ctx)
			if err != nil {
				spqrlog.Logger.PrintError(err)
			}
			wg.Done()
		}(wg)

		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			err := app.ServGrpc(ctx)
			if err != nil {
				spqrlog.Logger.PrintError(err)
			}
			wg.Done()
		}(wg)

		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			err := app.ProcADM(ctx)
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
