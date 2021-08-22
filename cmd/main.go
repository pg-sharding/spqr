package main

import (
	"fmt"

	"github.com/spf13/cobra"
)


var RootCmd = &cobra.Command{
	Use: "app",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("RUN ME!")
	},
}

func main() {
	RootCmd.Execute()
}