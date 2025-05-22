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
	SilenceUsage:  false,
	SilenceErrors: false,
}

var cfgPath string
var addr string
var port string
var loglevel string

var runCmd = &cobra.Command{
	Use: "run",
	RunE: func(cmd *cobra.Command, args []string) error {
		w := NewWorldMock(addr, port)

		spqrlog.ReloadLogger("", loglevel, false)

		return w.Run()
	},
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&cfgPath, "config", "c", "/etc/spqr/worldmock.yaml", "path to config file")
	rootCmd.PersistentFlags().StringVarP(&addr, "addr", "a", "localhost", "addr to listen")
	rootCmd.PersistentFlags().StringVarP(&port, "port", "p", "6432", "port to listen")
	rootCmd.PersistentFlags().StringVarP(&loglevel, "log-level", "l", "info", "log level ")
	rootCmd.AddCommand(runCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		spqrlog.Zero.Fatal().Err(err).Msg("")
	}
}
