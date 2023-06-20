package main

import (
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:  "worldmock run",
	Long: "Stateless Postgres Mocked World Shard",
	CompletionOptions: cobra.CompletionOptions{
		DisableDefaultCmd: true,
	},
	SilenceUsage:  true,
	SilenceErrors: true,
}

var cfgPath string
var addr string

var runCmd = &cobra.Command{
	Use: "run",
	RunE: func(cmd *cobra.Command, args []string) error {
		w := NewWorldMock(addr)
		return w.Run()
	},
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&cfgPath, "config", "c", "/etc/spqr/worldmock.yaml", "path to config file")
	rootCmd.PersistentFlags().StringVarP(&addr, "addr", "a", "localhost", "addr to listen")
	rootCmd.AddCommand(runCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		spqrlog.Logger.FatalOnError(err)
	}
}
