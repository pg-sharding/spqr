package main

import (
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/test/logproxy"
	"github.com/spf13/cobra"
)

var (
	host   string
	port   string
	user   string
	dbname string
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
	Short: "start proxy log writing session",
	RunE: func(cmd *cobra.Command, args []string) error {
		prox := logproxy.NewProxy(host, port)
		prox.Run()

		return nil
	},
	SilenceUsage:  false,
	SilenceErrors: false,
}

func init() {
	addRouterCmd.PersistentFlags().StringVarP(&host, "host", "H", "localhost", `database server host (default: "localhost")`)
	addRouterCmd.PersistentFlags().StringVarP(&port, "port", "p", "5432", `database server port (default: 5432)`)
	addRouterCmd.PersistentFlags().StringVarP(&user, "user", "U", "postgres", `database server user (default: postgres)`)
	addRouterCmd.PersistentFlags().StringVarP(&dbname, "dbname", "d", "postgres", `database name to connect to (default: postgres)`)

	/* --- Router cmds --- */
	rootCmd.AddCommand(addRouterCmd)
	/* ------------------- */
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		spqrlog.Zero.Err(err).Msg("")
	}
}
