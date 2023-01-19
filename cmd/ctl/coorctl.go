package main

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/spqrlog"

	protos "github.com/pg-sharding/spqr/pkg/protos"
)

var coordinatorEndpoint string

func DialCoordinator(r *topology.Router) (*grpc.ClientConn, error) {
	// TODO: add creds
	return grpc.Dial(r.Address, grpc.WithInsecure())
}

var rootCmd = &cobra.Command{
	Use: "coorctl -e localhost:7003",
	CompletionOptions: cobra.CompletionOptions{
		DisableDefaultCmd: true,
	},
	SilenceUsage:  false,
	SilenceErrors: false,
}

var runCmd = &cobra.Command{
	Use:   "ListRouters",
	Short: "run routers in topology",
	RunE: func(cmd *cobra.Command, args []string) error {
		spqrlog.Logger.Printf(spqrlog.DEBUG3, "dialing coordinator on %s", coordinatorEndpoint)
		internalR := &topology.Router{
			Address: coordinatorEndpoint,
		}

		cc, err := DialCoordinator(internalR)
		if err != nil {
			return err
		}

		rCl := protos.NewRouterServiceClient(cc)
		if resp, err := rCl.ListRouters(context.Background(), &protos.ListRoutersRequest{}); err == nil {
			fmt.Printf("-------------------------------------\n")
			fmt.Printf("%d routers found\n", len(resp.Routers))

			for _, router := range resp.Routers {
				fmt.Printf("router %s serving on address %s\n", router.Id, router.Adress)
			}

			fmt.Printf("-------------------------------------\n")
		} else {
			spqrlog.Logger.PrintError(err)
		}
		return nil
	},
	SilenceUsage:  false,
	SilenceErrors: false,
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&coordinatorEndpoint, "endpoint", "e", "localhost:7003", "coordinator endpoint")
	rootCmd.AddCommand(runCmd)
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		spqrlog.Logger.FatalOnError(err)
	}
}

func main() {
	Execute()
}
