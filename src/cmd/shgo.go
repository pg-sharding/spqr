package shgo

import (
	"fmt"

	"github.com/spf13/cobra"
)

var RootCmd = &cobra.Command{
	Use: "shgo",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("RUN ME!")
	},
}

func init() {
}
