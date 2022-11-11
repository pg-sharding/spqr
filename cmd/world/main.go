package main

import (
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/spf13/cobra"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/world"
)

var rootCmd = &cobra.Command{
	Use:  "world run ",
	Long: "Stateless Postgres ParamsQuerySuf RuleRouter",
	CompletionOptions: cobra.CompletionOptions{
		DisableDefaultCmd: true,
	},
	SilenceUsage:  true,
	SilenceErrors: true,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		spqrlog.Logger.Fatal(err)
	}
}

var cfgPath string

var ctlCmd = &cobra.Command{
	Use: "run",
	RunE: func(cmd *cobra.Command, args []string) error {
		rcfg, err := config.LoadRouterCfg(cfgPath)
		if err != nil {
			return err
		}

		w := world.NewWorld(&rcfg)

		if err := w.Run(); err != nil {
			spqrlog.Logger.PrintError(err)
		}

		return err
	},
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&cfgPath, "config", "c", "/etc/spqr/world.yaml", "path to config file")
	rootCmd.AddCommand(ctlCmd)
}

func main() {
	Execute()
}
