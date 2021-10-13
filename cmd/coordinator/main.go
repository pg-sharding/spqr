package main

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
)

var rootCmd = &cobra.Command{
	Use:   "world run --config `path-to-config`",
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


func main() {
	Execute()
}
