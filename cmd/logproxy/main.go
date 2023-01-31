package main

import (
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/test/logproxy"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:  "logproxy run",
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

var runCmd = &cobra.Command{
	Use: "run",
	RunE: func(cmd *cobra.Command, args []string) error {
		pr := &logproxy.Proxy{}
		return pr.Run()
	},
}

func init() {
	rootCmd.AddCommand(runCmd)
}

func main() {
	Execute()
}
