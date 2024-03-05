package main

import (
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
	Short: "spqr-coordinator",
	Long:  "spqr-coordinator",
	CompletionOptions: cobra.CompletionOptions{
		DisableDefaultCmd: true,
	},
	Version:       pkg.SpqrVersionRevision,
	SilenceUsage:  false,
	SilenceErrors: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := config.LoadBalancerCfg(cfgPath); err != nil {
			return err
		}

		balancer, err := provider.NewBalancer()
		if err != nil {
			return err
		}
		app := app.NewApp(balancer)
		return app.Run()
	},
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&cfgPath, "config", "c", "/etc/spqr/balancer.yaml", "path to config file")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
	}
}
