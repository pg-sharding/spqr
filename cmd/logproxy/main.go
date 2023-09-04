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
	file   string
)

var rootCmd = &cobra.Command{
	Use: "logproxy",
	CompletionOptions: cobra.CompletionOptions{
		DisableDefaultCmd: true,
	},
	SilenceUsage:  false,
	SilenceErrors: false,
}

var startProxySessionCmd = &cobra.Command{
	Use:   "run",
	Short: "start proxy log writing session",
	RunE: func(cmd *cobra.Command, args []string) error {
		prox := logproxy.NewProxy(host, port)
		prox.Run()

		return nil
	},
	SilenceUsage:  false,
	SilenceErrors: false,
}

var replayLogsCmd = &cobra.Command{
	Use:   "replay",
	Short: "replay written logs to db",
	RunE: func(cmd *cobra.Command, args []string) error {
		prox := logproxy.NewProxy(host, port)
		err := prox.ReplayLogs(host, port, user, file, dbname)

		if err != nil {
			return err
		}

		return nil
	},
	SilenceUsage:  false,
	SilenceErrors: false,
}

func init() {
	startProxySessionCmd.PersistentFlags().StringVarP(&host, "host", "H", "localhost", `database server host (default: "localhost")`)
	startProxySessionCmd.PersistentFlags().StringVarP(&port, "port", "p", "5432", `database server port (default: 5432)`)
	startProxySessionCmd.PersistentFlags().StringVarP(&user, "user", "U", "postgres", `database server user (default: postgres)`)
	startProxySessionCmd.PersistentFlags().StringVarP(&dbname, "dbname", "d", "postgres", `database name to connect to (default: postgres)`)

	replayLogsCmd.PersistentFlags().StringVarP(&host, "host", "H", "localhost", `database server host (default: "localhost")`)
	replayLogsCmd.PersistentFlags().StringVarP(&port, "port", "p", "5432", `database server port (default: 5432)`)
	replayLogsCmd.PersistentFlags().StringVarP(&user, "user", "U", "postgres", `database server user (default: postgres)`)
	replayLogsCmd.PersistentFlags().StringVarP(&dbname, "dbname", "d", "postgres", `database name to connect to (default: postgres)`)
	replayLogsCmd.PersistentFlags().StringVarP(&file, "logfile", "l", "", `file to read logs from`)

	/* --- Router cmds --- */
	rootCmd.AddCommand(startProxySessionCmd)
	rootCmd.AddCommand(replayLogsCmd)
	/* ------------------- */
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		spqrlog.Zero.Err(err).Msg("")
	}
}
