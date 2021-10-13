package main

import (
	"github.com/pg-sharding/spqr/coordinator/provider"
	"github.com/pg-sharding/spqr/qdb/qdb/mem"
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
)

var rootCmd = &cobra.Command{
	Use:   "coordinator run --config `path-to-config`",
	CompletionOptions: cobra.CompletionOptions{
		DisableDefaultCmd: true,
	},
	SilenceUsage:  true,
	SilenceErrors: true,
	Run: func(cmd *cobra.Command, args []string) {
		db, _ := mem.NewQrouterDBMem()
		c := provider.NewCoordinator(db)
		c.Run()
	},
}


func Execute() {
	if err := rootCmd.Execute(); err != nil {
		tracelog.ErrorLogger.Fatal(err)
	}
}


func main() {
	Execute()
}
