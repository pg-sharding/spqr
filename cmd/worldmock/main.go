package main

import (
	"github.com/pg-sharding/spqr/test/worldmock"
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
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
var addr string

var ctlCmd = &cobra.Command{
	Use: "run",
	RunE: func(cmd *cobra.Command, args []string) error {

		w := worldmock.NewWorldMock(addr)
		err := w.Run()
		if err != nil {
			tracelog.ErrorLogger.FatalOnError(err)
		}

		return err
	},
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&cfgPath, "config", "c", "/etc/worldmock/config.yaml", "path to config file")
	rootCmd.PersistentFlags().StringVarP(&addr, "addr", "a", "localhost", "addr to listen")
	rootCmd.AddCommand(ctlCmd)
}

func main() {
	Execute()
}
