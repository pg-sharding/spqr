package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path"
	"runtime"
	"runtime/pprof"
	"sync"
	"syscall"

	coordApp "github.com/pg-sharding/spqr/coordinator/app"
	"github.com/pg-sharding/spqr/pkg"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/coord"
	"github.com/pg-sharding/spqr/pkg/datatransfers"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/app"
	"github.com/pg-sharding/spqr/router/instance"
	"github.com/pkg/errors"
	"github.com/sevlyar/go-daemon"
	"github.com/spf13/cobra"
)

var (
	rcfgPath string
	ccfgPath string

	logLevel              string
	memqdbBackupPath      string
	routerPort            int
	routerROPort          int
	adminPort             int
	grpcPort              int
	defaultRouteBehaviour string

	enhancedMultishardProcessing bool

	showNoticeMessages bool
	pgprotoDebug       bool
	profileFile        string
	cpuProfile         bool
	memProfile         bool

	qdbImpl       string
	daemonize     bool
	console       bool
	prettyLogging bool
	gomaxprocs    int

	withCoord bool

	rootCmd = &cobra.Command{
		Use:   "spqr-router run --config `path-to-config-folder`",
		Short: "spqr-router",
		Long:  "spqr-router",
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd: true,
		},
		Version:       pkg.SpqrVersionRevision,
		SilenceUsage:  false,
		SilenceErrors: false,
	}
)

func init() {
	// Router and coordinator config paths
	rootCmd.PersistentFlags().StringVarP(&rcfgPath, "config", "c", "/etc/spqr/router.yaml", "path to router config file")
	rootCmd.PersistentFlags().StringVarP(&ccfgPath, "coordinator-config", "", "/etc/spqr/coordinator.yaml", "path to coordinator config file")
	rootCmd.PersistentFlags().BoolVarP(&withCoord, "with-coordinator", "", false, "start spqr coordinator in separate goroutine")

	// Overload for values from the config file
	rootCmd.PersistentFlags().StringVarP(&logLevel, "log-level", "l", "", "overload for `log_level` option in router config")
	rootCmd.PersistentFlags().StringVarP(&memqdbBackupPath, "memqdb-backup-path", "", "", "overload for `memqdb_backup_path` option in router config")
	rootCmd.PersistentFlags().IntVarP(&routerPort, "router-port", "", 0, "overload for `router_port` option in router config")
	rootCmd.PersistentFlags().IntVarP(&routerROPort, "router-ro-port", "", 0, "overload for `router_ro_port` option in router config")
	rootCmd.PersistentFlags().IntVarP(&adminPort, "admin-port", "", 0, "overload for `admin_console_port` option in router config")
	rootCmd.PersistentFlags().IntVarP(&grpcPort, "grpc-port", "", 0, "overload for `grpc_api_port` option in router config")
	rootCmd.PersistentFlags().StringVarP(&defaultRouteBehaviour, "default-route-behaviour", "", "", "overload for `default_route_behaviour` option in router config")
	rootCmd.PersistentFlags().BoolVarP(&showNoticeMessages, "show-notice-messages", "", false, "overload for `show_notice_messages` option in router config")
	rootCmd.PersistentFlags().BoolVarP(&pgprotoDebug, "pgproto-debug", "", false, "overload for `pgproto_debug` option in router config")

	// Flags for profiling and debug
	rootCmd.PersistentFlags().StringVarP(&profileFile, "profile-file", "p", "/etc/spqr/router.prof", "path to profile file")
	rootCmd.PersistentFlags().BoolVar(&cpuProfile, "cpu-profile", false, "profile cpu or not")
	rootCmd.PersistentFlags().BoolVar(&memProfile, "mem-profile", false, "profile mem or not")

	// Other flags
	rootCmd.PersistentFlags().StringVarP(&qdbImpl, "qdb-impl", "", "etcd", "which implementation of QDB to use")
	rootCmd.PersistentFlags().BoolVarP(&daemonize, "daemonize", "d", false, "run as a daemon or not. Opposite of `console`")
	rootCmd.PersistentFlags().BoolVarP(&console, "console", "", false, "run as a console app or not. Opposite of `daemonize`")
	rootCmd.PersistentFlags().BoolVarP(&prettyLogging, "pretty-log", "P", false, "enables pretty logging")
	rootCmd.PersistentFlags().IntVarP(&gomaxprocs, "gomaxprocs", "", 0, "GOMAXPROCS value")

	// Query processing
	rootCmd.PersistentFlags().BoolVarP(&enhancedMultishardProcessing, "enhanced_multishard_processing", "e", false, "enables SPQR query processing engine V2")

	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(testCmd)
}

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "run router",
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := config.LoadRouterCfg(rcfgPath)
		if err != nil {
			return err
		}

		if err := applyOverrides(cmd, cfg); err != nil {
			return err
		}

		log.Println("Running config:", cfg.String())

		if cfg.EnableRoleSystem {
			if cfg.RolesFile == "" {
				return fmt.Errorf("role system enabled but no roles file specified, see `enable_role_system` and `roles_file` in config")
			}
			rolesCfgStr, err := config.LoadRolesCfg(cfg.RolesFile)
			if err != nil {
				return err
			}
			log.Println("Running roles config:", rolesCfgStr)
		}

		spqrlog.ReloadLogger(cfg.LogFileName, cfg.LogLevel, cfg.PrettyLogging)
		spqrlog.ReloadSLogger(cfg.LogMinDurationStatement)

		if console && daemonize {
			return fmt.Errorf("simultaneous use of `console` and `daemonize`. Abort")
		}

		if !console && (cfg.Daemonize || daemonize) {
			ctx := &daemon.Context{
				PidFileName: cfg.PidFileName,
				PidFilePerm: 0644,
				WorkDir:     "./",
				Umask:       027,
				Args:        args,
			}

			d, err := ctx.Reborn()
			if err != nil {
				log.Fatal("Unable to run: ", err)
			}
			if d != nil {
				return nil
			}

			defer func() {
				if err := ctx.Release(); err != nil {
					spqrlog.Zero.Error().Msg("")
					spqrlog.Zero.Error().Err(err).Msg("")
				}
			}()

			spqrlog.Zero.Debug().Msg("daemon started")
		}

		if cfg.UseCoordinatorInit && cfg.UseInitSQL {
			return fmt.Errorf("cannot use initSQL and coordinator-based init simultaneously")
		}

		ctx, cancelCtx := context.WithCancel(context.Background())
		defer cancelCtx()

		var pprofCpuFile *os.File
		var pprofMemFile *os.File

		if cpuProfile {
			spqrlog.Zero.Info().Msg("starting cpu profile")
			pprofCpuFile, err = os.Create(path.Join(path.Dir(profileFile), "cpu"+path.Base(profileFile)))

			if err != nil {
				spqrlog.Zero.Info().
					Err(err).
					Msg("got an error while starting cpu profile")
				return err
			}

			if err := pprof.StartCPUProfile(pprofCpuFile); err != nil {
				spqrlog.Zero.Info().
					Err(err).
					Msg("got an error while starting cpu profile")
				return err
			}
		}

		if memProfile {
			spqrlog.Zero.Info().Msg("starting mem profile")
			pprofMemFile, err = os.Create(path.Join(path.Dir(profileFile), "mem"+path.Base(profileFile)))
			if err != nil {
				spqrlog.Zero.Info().
					Err(err).
					Msg("got an error while starting mem profile")
				return err
			}
		}

		if gomaxprocs > 0 {
			runtime.GOMAXPROCS(gomaxprocs)
		}

		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR1, syscall.SIGUSR2)

		router, err := instance.NewRouter(ctx, os.Getenv("NOTIFY_SOCKET"))
		if err != nil {
			return errors.Wrap(err, "router failed to start")
		}

		app := app.NewApp(router)

		if rcfgPath != "" {
			if err := datatransfers.LoadConfig(rcfgPath); err != nil {
				return err
			}
		}
		if cfg.WithCoordinator {
			go func() {
				if err := func() error {
					cfg, err := config.LoadCoordinatorCfg(ccfgPath)
					if err != nil {
						return err
					}
					log.Println("Running coordinator config:", cfg)

					db, err := qdb.NewXQDB(qdbImpl)
					if err != nil {
						return err
					}

					frTLS, err := config.CoordinatorConfig().FrontendTLS.Init(config.CoordinatorConfig().Host)
					if err != nil {
						return fmt.Errorf("init frontend TLS: %w", err)
					}

					coordinator, err := coord.NewClusteredCoordinator(frTLS, db)
					if err != nil {
						return err
					}

					app := coordApp.NewApp(coordinator)
					return app.Run(false)
				}(); err != nil {
					spqrlog.Zero.Error().Err(err).Msg("")
				}
			}()
		}
		go func() {
			defer cancelCtx()
			for {
				s := <-sigs
				spqrlog.Zero.Info().Str("signal", s.String()).Msg("received signal")

				switch s {
				case syscall.SIGUSR1:
					spqrlog.ReloadLogger(cfg.LogFileName, cfg.LogLevel, cfg.PrettyLogging)
					spqrlog.ReloadSLogger(cfg.LogMinDurationStatement)
				case syscall.SIGUSR2:
					if cpuProfile {
						// write profile
						pprof.StopCPUProfile()
						spqrlog.Zero.Info().Str("fname", pprofCpuFile.Name()).Msg("writing cpu prof")

						if err := pprofCpuFile.Close(); err != nil {
							spqrlog.Zero.Error().Err(err).Msg("")
						}
					}
					if memProfile {
						// write profile
						spqrlog.Zero.Info().Str("fname", pprofMemFile.Name()).Msg("writing mem prof")

						if err := pprof.WriteHeapProfile(pprofMemFile); err != nil {
							spqrlog.Zero.Error().Err(err).Msg("")
						}
						if err := pprofMemFile.Close(); err != nil {
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
					spqrlog.ReloadLogger(cfg.LogFileName, cfg.LogLevel, cfg.PrettyLogging)
					spqrlog.ReloadSLogger(cfg.LogMinDurationStatement)
				case syscall.SIGINT, syscall.SIGTERM:
					if cpuProfile {
						// write profile
						pprof.StopCPUProfile()

						spqrlog.Zero.Info().Msg("writing cpu prof")
						if err := pprofCpuFile.Close(); err != nil {
							spqrlog.Zero.Error().Err(err).Msg("")
						}
					}

					if memProfile {
						// write profile
						spqrlog.Zero.Info().Msg("writing mem prof")

						if err := pprof.WriteHeapProfile(pprofMemFile); err != nil {
							spqrlog.Zero.Error().Err(err).Msg("")
						}
						if err := pprofMemFile.Close(); err != nil {
							spqrlog.Zero.Error().Err(err).Msg("")
						}
					}
					return
				default:
					return
				}
			}
		}()

		/* initialize metadata */
		if cfg.UseInitSQL {
			i := instance.NewInitSQLMetadataBootstrapper(cfg.InitSQL, cfg.ExitOnInitSQLError)
			if err := i.InitializeMetadata(ctx, router); err != nil {
				return err
			}
		} else if cfg.UseCoordinatorInit {
			/* load config if not yet */
			_, err := config.LoadCoordinatorCfg(ccfgPath)
			if err != nil {
				return err
			}
			e := instance.NewEtcdMetadataBootstrapper(config.CoordinatorConfig().QdbAddr)
			if err := e.InitializeMetadata(ctx, router); err != nil {
				return err
			}
		} else {
			/* TODO: maybe error-out? */
			router.Initialize()
		}

		errCh := make(chan error)

		go func() {
			for {
				<-errCh
				os.Exit(1)
			}
		}()

		wg := &sync.WaitGroup{}

		wg.Go(func() {
			err := app.ServeRouter(ctx)
			if err != nil {
				spqrlog.Zero.Error().Err(err).Msg("failed to serve SQL console")
				errCh <- err
			}
		})

		wg.Go(func() {
			err := app.ServeGrpcApi(ctx)
			if err != nil {
				spqrlog.Zero.Error().Err(err).Msg("failed to serve gRPC API")
				errCh <- err
			}
		})

		wg.Go(func() {
			err := app.ServeAdminConsole(ctx)
			if err != nil {
				spqrlog.Zero.Error().Err(err).Msg("failed to serve SQL administrative console")
				errCh <- err
			}
		})

		wg.Go(func() {
			err := app.ServiceUnixSocket(ctx)
			if err != nil {
				spqrlog.Zero.Error().Err(err).Msg("failed to serve unix socket")
				errCh <- err
			}
		})

		// run pprof without wait group
		go func() {
			log.Println(http.ListenAndServe("localhost:6060", nil))
		}()

		wg.Wait()

		return nil
	},
}

var testCmd = &cobra.Command{
	Use:   "test-config {path-to-config | -c path-to-config}",
	Short: "Load, validate and print the given config file",
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) > 0 {
			rcfgPath = args[0]
		}
		cfg, err := config.LoadRouterCfg(rcfgPath)
		if err != nil {
			return err
		}

		if err := applyOverrides(cmd, cfg); err != nil {
			return err
		}

		fmt.Println(cfg.String())
		return nil
	},
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		spqrlog.Zero.Fatal().Err(err).Msg("")
	}
}
