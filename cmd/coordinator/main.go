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
	Use:   "[coordinator bin] ctl --config `path-to-data-folder`",
	Short: "SPQR",
	Long:  "Stateless Postgres Query Router",
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
func init() {
	rootCmd.PersistentFlags().StringVarP(&ccfgPath, "coordinator-config", "", "/etc/coordinator/config.yaml", "path to config file")
	rootCmd.AddCommand(ctlCmd)
}

var ctlCmd = &cobra.Command{
	Use:   "ctl",
	Short: "ctl SPQR",
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Println("OK")
		return nil
	},
}

func main() {
	Execute()
}
