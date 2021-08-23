package cmd

import (
	"fmt"
	"os"
	"sync"

	"github.com/pg-sharding/spqr/app"
	"github.com/pg-sharding/spqr/internal/core"
	"github.com/pg-sharding/spqr/internal/r"
	"github.com/pg-sharding/spqr/internal/spqr"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v2"
)

var cfgFile string

func init() {
	cobra.OnInitialize(initConfig)

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "path to config file")
	
	rootCmd.AddCommand(runCmd)
}

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "run sqpr",
	Long:  `All software has versions. This is Hugo's`,
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Println("running!")
		f, err := os.Open(cfgFile)
		if err != nil {
			return err
		}
		defer f.Close()

		fmt.Println("i open file", cfgFile)

		var cfg spqr.GlobConfig
		decoder := yaml.NewDecoder(f)
		err = decoder.Decode(&cfg)
		if err != nil {
			return err
		}

		fmt.Println("PARSED:", cfg.Addr)

		rt, err := core.NewRouter(cfg.RouterCfg)
		if err != nil {
			return err
		}

		spqr, err := spqr.NewSpqr(
			cfg,
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

			if err != nil {
				panic(err) // TODO remove panic
			}

			wg.Done()
		}(wg)

		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			err := app.ServHttp()

			if err != nil {
				panic(err) // TODO remove panic
			}

			wg.Done()
		}(wg)

		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			err := app.ProcADM()
			if err != nil {
				panic(err) // TODO remove panic
			}

			wg.Done()
		}(wg)

		wg.Wait()

		return nil
	},
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		fmt.Println("Please pass config path with --config")
		os.Exit(1)
	}

	viper.AutomaticEnv()
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}