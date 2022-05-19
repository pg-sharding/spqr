package main

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/world"
)

var rootCmd = &cobra.Command{
	Use:  "world run ",
	Long: "Stateless Postgres Query Rrouter",
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

var cfgPath string

var ctlCmd = &cobra.Command{
	Use: "run",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := config.LoadRouterCfg(cfgPath); err != nil {
			tracelog.ErrorLogger.FatalOnError(err)
		}

		w := world.NewWorld()
		err := w.Run()
		if err != nil {
			tracelog.ErrorLogger.FatalOnError(err)
		}

		return err
	},
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&cfgPath, "config", "c", "/etc/world/config.yaml", "path to config file")
	rootCmd.AddCommand(ctlCmd)
}

func main() {
	Execute()
}
