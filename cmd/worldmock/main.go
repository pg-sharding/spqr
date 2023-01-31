package main

import (
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/test/worldmock"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:  "worldmock run",
	Long: "Stateless Postgres ParamsQuerySuf Router",
	CompletionOptions: cobra.CompletionOptions{
		DisableDefaultCmd: true,
	},
	SilenceUsage:  true,
	SilenceErrors: true,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		spqrlog.Logger.FatalOnError(err)
	}
}

var cfgPath string
var addr string

var runCmd = &cobra.Command{
	Use: "run",
	RunE: func(cmd *cobra.Command, args []string) error {
		w := worldmock.NewWorldMock(addr)
		return w.Run()
	},
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&cfgPath, "config", "c", "/etc/spqr/worldmock.yaml", "path to config file")
	rootCmd.PersistentFlags().StringVarP(&addr, "addr", "a", "localhost", "addr to listen")
	rootCmd.AddCommand(runCmd)
}

func main() {
	Execute()
}
