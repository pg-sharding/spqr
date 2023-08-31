package main

import (
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/test/logproxy"
	"github.com/spf13/cobra"
)

var (
	coordinatorEndpoint string

	routerEndpoint string
	routerID       string
)

var rootCmd = &cobra.Command{
	Use: "root",
	CompletionOptions: cobra.CompletionOptions{
		DisableDefaultCmd: true,
	},
	SilenceUsage:  false,
	SilenceErrors: false,
}

var addRouterCmd = &cobra.Command{
	Use:   "first",
	Short: "add routers in topology",
	RunE: func(cmd *cobra.Command, args []string) error {
		spqrlog.Zero.Debug().
			Msg("first")

		prox := logproxy.Proxy{}
		prox.Run()

		return nil
	},
	SilenceUsage:  false,
	SilenceErrors: false,
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&coordinatorEndpoint, "endpoint", "e", "localhost:7003", "coordinator endpoint")

	addRouterCmd.PersistentFlags().StringVarP(&routerEndpoint, "router-endpoint", "u", "000", "router endpoint to add")
	addRouterCmd.PersistentFlags().StringVarP(&routerID, "router-id", "i", "111", "router id to add")

	/* --- Router cmds --- */
	rootCmd.AddCommand(addRouterCmd)
	/* ------------------- */
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		spqrlog.Zero.Err(err).Msg("")
	}
}
