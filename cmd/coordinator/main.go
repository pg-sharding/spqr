package main

import (
	"github.com/pg-sharding/spqr/coordinator/provider"
	"github.com/pg-sharding/spqr/qdb/qdb/mem"
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
)

var rootCmd = &cobra.Command{
	Use: "keyrangeservice --config `path-to-config`",
	CompletionOptions: cobra.CompletionOptions{
		DisableDefaultCmd: true,
	},
	SilenceUsage:  true,
	SilenceErrors: true,
	Run: func(cmd *cobra.Command, args []string) {
		db, _ := mem.NewQrouterDBMem()
		c := provider.NewCoordinator(db)
		_ = c.Run()
	},
}
var cfgPath string

func init() {
	rootCmd.PersistentFlags().StringVarP(&cfgPath, "config", "c", "/etc/keyrangeservice/config.yaml", "path to config file")

}
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		tracelog.ErrorLogger.Fatal(err)
	}
}
func main() {
	Execute()
}
