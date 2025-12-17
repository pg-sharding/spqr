package main

import (
	"fmt"
	"log"

	"github.com/pg-sharding/spqr/balancer/app"
	"github.com/pg-sharding/spqr/balancer/provider"
	"github.com/pg-sharding/spqr/pkg"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/spf13/cobra"
)

var (
	cfgPath string
)

var rootCmd = &cobra.Command{
	Use:   "spqr-balancer run --config `path-to-config`",
	Short: "spqr-balancer",
	Long:  "spqr-balancer",
	CompletionOptions: cobra.CompletionOptions{
		DisableDefaultCmd: true,
	},
	Version:       pkg.SpqrVersionRevision,
	SilenceUsage:  false,
	SilenceErrors: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		cfgStr, err := config.LoadBalancerCfg(cfgPath)
		if err != nil {
			return err
		}
		log.Println("Running config:", cfgStr)

		// TODO add config.BalancerConfig().LogFileName
		spqrlog.ReloadLogger("", config.BalancerConfig().LogLevel, false)

		balancer, err := provider.NewBalancer()
		if err != nil {
			return err
		}
		app := app.NewApp(balancer)
		return app.Run()
	},
}

var testCmd = &cobra.Command{
	Use:   "test-config {path-to-config | -c path-to-config}",
	Short: "Load, validate and print the given config file",
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) > 0 {
			cfgPath = args[0]
		}
		cfgStr, err := config.LoadBalancerCfg(cfgPath)
		if err != nil {
			return err
		}
		fmt.Println(cfgStr)
		return nil
	},
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&cfgPath, "config", "c", "/etc/spqr/balancer.yaml", "path to config file")
	rootCmd.AddCommand(testCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
	}
}
