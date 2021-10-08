
package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
)

var (
	ccfgPath string
)

func init() {
	rootCmd.PersistentFlags().StringVarP(&ccfgPath, "coordinator-config", "", "/etc/coordinator/config.yaml", "path to config file")
	rootCmd.AddCommand(ctlCmd)
}

var ctlCmd = &cobra.Command{
	Use:   "ctl",
	Short: "ctl SPQR",
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Println("OK");
		return nil
	},
}
