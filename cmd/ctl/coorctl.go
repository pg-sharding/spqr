package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/spqrlog"

	protos "github.com/pg-sharding/spqr/pkg/protos"
)

// TDB: move to util
func randomHex(n int) (string, error) {
	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

var (
	coordinatorEndpoint string
	routerEndpoint      string
	routerID            string
)

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

var addRouterCmd = &cobra.Command{
	Use:   "AddRouter",
	Short: "add routers in topology",
	RunE: func(cmd *cobra.Command, args []string) error {
		spqrlog.Logger.Printf(spqrlog.DEBUG3, "dialing coordinator on %s", coordinatorEndpoint)
		internalR := &topology.Router{
			Address: coordinatorEndpoint,
		}

		cc, err := DialCoordinator(internalR)
		if err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}

		if routerID == "" {
			routerID, err = randomHex(6)

			if err != nil {
				spqrlog.Logger.PrintError(err)
				return err
			}
		}

		rCl := protos.NewRouterServiceClient(cc)
		if resp, err := rCl.AddRouter(context.Background(), &protos.AddRouterRequest{
			Router: &protos.Router{
				Id:      routerID,
				Address: routerEndpoint,
			},
		}); err == nil {
			fmt.Printf("-------------------------------------\n")
			fmt.Printf("create router with id: %s\n", resp.Id)
			fmt.Printf("-------------------------------------\n")
		} else {
			spqrlog.Logger.PrintError(err)
		}
		return nil
	},
	SilenceUsage:  false,
	SilenceErrors: false,
}

var listRouterCmd = &cobra.Command{
	Use:   "ListRouters",
	Short: "list running routers in current topology",
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
				fmt.Printf("router %s serving on address %s\n", router.Id, router.Address)
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

	addRouterCmd.PersistentFlags().StringVarP(&routerEndpoint, "router-endpoint", "", "", "router endpoint to add")
	addRouterCmd.PersistentFlags().StringVarP(&routerID, "router-id", "", "", "router id to add")

	rootCmd.AddCommand(listRouterCmd)
	rootCmd.AddCommand(addRouterCmd)
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		spqrlog.Logger.FatalOnError(err)
	}
}

func main() {
	Execute()
}
