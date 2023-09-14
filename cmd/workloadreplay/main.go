package main

import (
	"log"

	"github.com/pg-sharding/spqr/pkg/workloadreplay"
	"github.com/spf13/cobra"
)

var (
	//replay cmd
	host   string
	port   string
	user   string
	dbname string
	file   string
)

var rootCmd = &cobra.Command{
	Use: "workload",
	CompletionOptions: cobra.CompletionOptions{
		DisableDefaultCmd: true,
	},
	SilenceUsage:  false,
	SilenceErrors: false,
}

var replayLogsCmd = &cobra.Command{
	Use:   "replay",
	Short: "replay written logs to db",
	RunE: func(cmd *cobra.Command, args []string) error {
		err := workloadreplay.ReplayLogs(host, port, user, dbname, file)
		return err
	},
	SilenceUsage:  false,
	SilenceErrors: false,
}

func init() {
	replayLogsCmd.PersistentFlags().StringVarP(&host, "host", "H", "localhost", `database server host (default: "localhost")`)
	replayLogsCmd.PersistentFlags().StringVarP(&port, "port", "p", "5432", `database server port (default: 5432)`)
	replayLogsCmd.PersistentFlags().StringVarP(&user, "user", "U", "postgres", `database server user (default: postgres)`)
	replayLogsCmd.PersistentFlags().StringVarP(&dbname, "dbname", "d", "postgres", `database name to connect to (default: postgres)`)
	replayLogsCmd.PersistentFlags().StringVarP(&file, "logfile", "l", "", `file to read logs from`)

	/* --- Router cmds --- */
	rootCmd.AddCommand(replayLogsCmd)
	/* ------------------- */
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
