package cmd

import (
	"fmt"
	"os"
   
	"github.com/spf13/cobra"
)


var rootCmd = &cobra.Command{
	Use: "spqr run --config `config-path`",
	Short: "SPQR",
	Long: "Stateless Postgres Query Router",
	CompletionOptions: cobra.CompletionOptions{
		DisableDefaultCmd:   true,
	},
	SilenceUsage: true,
	SilenceErrors: true,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}