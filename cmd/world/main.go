package main

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
)

var (
	ccfgPath string
)

var rootCmd = &cobra.Command{
	Short: "SPQR",
	Long:  "Stateless Postgres Query Rrouter",
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

var ctlCmd = &cobra.Command{
	Use: "run",
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Println("OK")
		return nil
	},
}

func main() {
	Execute()
}
