package cmd

import (
	"os"
	"sync"

	"github.com/pg-sharding/spqr/app"
	"github.com/pg-sharding/spqr/internal/core"
	"github.com/pg-sharding/spqr/internal/r"
	"github.com/pg-sharding/spqr/internal/spqr"
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"gopkg.in/yaml.v2"
)

var configPath string
var config spqr.GlobConfig

func init() {
	cobra.OnInitialize(initConfig)

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.
	rootCmd.PersistentFlags().StringVarP(&configPath, "config", "c", "", "path to config file")
	
	rootCmd.AddCommand(runCmd)
}

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "run sqpr",
	Long:  `All software has versions. This is Hugo's`,
	RunE: func(cmd *cobra.Command, args []string) error {


		rt, err := core.NewRouter(config.RouterCfg)
		if err != nil {
			return err
		}

		spqr, err := spqr.NewSpqr(
			config,
			rt,
			r.NewR(),
		)
		if err != nil {
			return err
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

// initConfig reads in config
func initConfig() {
	// anyway viper is a dependency for cobra so why not
	if configPath != "" {
		tracelog.InfoLogger.Println("Parsing config from", configPath)
		file, err := os.Open(configPath)
		tracelog.ErrorLogger.FatalOnError(err)
		defer file.Close()

		tracelog.InfoLogger.Println("Decoding config")
		decoder := yaml.NewDecoder(file)
		err = decoder.Decode(&config)
		tracelog.ErrorLogger.FatalOnError(err)
	} else {
		tracelog.ErrorLogger.Fatal("Please pass config path with --config")
	}
}