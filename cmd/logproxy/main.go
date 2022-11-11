package main

import (
	"github.com/spf13/cobra"

	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/test/logproxy"
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
		spqrlog.Logger.Fatal(err)
	}
}

var runCmd = &cobra.Command{
	Use: "run",
	RunE: func(cmd *cobra.Command, args []string) error {

		pr := logproxy.NewProxy()
		err := pr.Run()
		if err != nil {
			spqrlog.Logger.Fatal(err)
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
