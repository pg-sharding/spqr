package main

import (
	"context"
	"log"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime/pprof"
	"sync"
	"syscall"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	router "github.com/pg-sharding/spqr/router"
	"github.com/pg-sharding/spqr/router/app"
	"github.com/pkg/errors"
	"github.com/sevlyar/go-daemon"
	"github.com/spf13/cobra"
)

var (
	rcfgPath    string
	saveProfie  bool
	profileFile string
	daemonize   bool
	logLevel	string
)

var rootCmd = &cobra.Command{
	Use:   "spqr-router run --config `path-to-config-folder`",
	Short: "sqpr-router",
	Long:  "spqr-router",
	CompletionOptions: cobra.CompletionOptions{
		DisableDefaultCmd: true,
	},
	SilenceUsage:  true,
	SilenceErrors: true,
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&rcfgPath, "config", "c", "/etc/spqr/router.yaml", "path to config file")
	rootCmd.PersistentFlags().StringVarP(&profileFile, "profile-file", "p", "/etc/spqr/router.prof", "path to profile file")
	rootCmd.PersistentFlags().BoolVarP(&daemonize, "daemonize", "d", false, "daemonize router binary or not")
	rootCmd.PersistentFlags().BoolVar(&saveProfie, "profile", false, "path to config file")
	rootCmd.PersistentFlags().StringVarP(&logLevel, "log-level", "l", "", "log level")
	rootCmd.AddCommand(runCmd)
}

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "run router",
	RunE: func(cmd *cobra.Command, args []string) error {
		rcfg, err := config.LoadRouterCfg(rcfgPath)
		if err != nil {
			return err
		}

		spqrlog.ReloadLogger(rcfg.LogFileName)

		// Logger
		rlogLevel := rcfg.LogLevel
		if logLevel != "" {
			rlogLevel = logLevel
		}

		if err := spqrlog.UpdateZeroLogLevel(rlogLevel); err != nil {
			return err
		}

		if rcfg.Daemonize || daemonize {
			cntxt := &daemon.Context{
				PidFileName: rcfg.PidFileName,
				PidFilePerm: 0644,
				WorkDir:     "./",
				Umask:       027,
				Args:        args,
			}

			d, err := cntxt.Reborn()
			if err != nil {
				log.Fatal("Unable to run: ", err)
			}
			if d != nil {
				return nil
			}

			defer func() {
				if err := cntxt.Release(); err != nil {
					spqrlog.Zero.Error().Msg("")
					spqrlog.Zero.Error().Err(err).Msg("")
				}
			}()

			spqrlog.Zero.Debug().Msg("daemon started")
		}

		ctx, cancelCtx := context.WithCancel(context.Background())
		defer cancelCtx()

		var pprofFile *os.File

		if saveProfie {
			spqrlog.Zero.Fatal().Msg("starting cpu profile")
			pprofFile, err = os.Create(profileFile)
			if err != nil {
				spqrlog.Zero.Fatal().
					Err(err).
					Msg("got an error while starting cpu profile")
				return err
			}
			spqrlog.Zero.Fatal().Str("file", profileFile).Msg("starting cpu profile")
			if err := pprof.StartCPUProfile(pprofFile); err != nil {
				spqrlog.Zero.Fatal().
					Err(err).
					Msg("got an error while starting cpu profile")
				return err
			}
		}

		sigs := make(chan os.Signal, 1)
                signal.Notify(sigs, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR1, syscall.SIGUSR2)

		router, err := router.NewRouter(ctx, &rcfg)
		if err != nil {
			return errors.Wrap(err, "router failed to start")
		}

		app := app.NewApp(router)

		go func() {
			defer cancelCtx()
			for {
				s := <-sigs
				spqrlog.Zero.Info().Str("signal", s.String()).Msg("received signal")

				switch s {
				case syscall.SIGUSR1:
					spqrlog.ReloadLogger(rcfg.LogFileName)
				case syscall.SIGUSR2:
					if saveProfie {
						// write profile
						pprof.StopCPUProfile()
						spqrlog.Zero.Fatal().Msg("writing cpu prof")

						if err := pprofFile.Close(); err != nil {
							spqrlog.Zero.Error().Err(err).Msg("")
						}
					}
					return
				case syscall.SIGHUP:
					// reread config file
					err := router.RuleRouter.Reload(rcfgPath)
					if err != nil {
						spqrlog.Zero.Error().Err(err).Msg("")
					}
					spqrlog.ReloadLogger(rcfg.LogFileName)
				case syscall.SIGINT, syscall.SIGTERM:
					if saveProfie {
						// write profile
						pprof.StopCPUProfile()

						spqrlog.Zero.Fatal().Msg("writing cpu prof")
						if err := pprofFile.Close(); err != nil {
							spqrlog.Zero.Error().Err(err).Msg("")
						}
					}
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
				spqrlog.Zero.Error().Err(err).Msg("")
			}
			wg.Done()
		}(wg)

		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			err := app.ServeGrpcApi(ctx)
			if err != nil {
				spqrlog.Zero.Error().Err(err).Msg("")
			}
			wg.Done()
		}(wg)

		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			err := app.ServeAdminConsole(ctx)
			if err != nil {
				spqrlog.Zero.Error().Err(err).Msg("")
			}
			wg.Done()
		}(wg)

		wg.Wait()

		return nil
	},
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		spqrlog.Zero.Fatal().Err(err).Msg("")
	}
}
