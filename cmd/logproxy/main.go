package main

import (
	"github.com/pg-sharding/spqr/test/logproxy"
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
)

var rootCmd = &cobra.Command{
	Use:  "logproxy run",
	Long: "Stateless Postgres Query Router",
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

var runCmd = &cobra.Command{
	Use: "run",
	RunE: func(cmd *cobra.Command, args []string) error {

		pr := logproxy.NewProxy()
		err := pr.Run()
		if err != nil {
			tracelog.ErrorLogger.FatalOnError(err)
		}

		return err
	},
}

func init() {
	rootCmd.AddCommand(runCmd)
}

func main() {
	Execute()
}
