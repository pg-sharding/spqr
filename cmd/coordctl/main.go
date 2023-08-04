package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/pg-sharding/spqr/pkg/models/topology"
	protos "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
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

	routerEndpoint string
	routerID       string

	shardID    string
	shardHosts []string
)

func DialCoordinator(r *topology.Router) (*grpc.ClientConn, error) {
	// TODO: add creds, remove WithInsecure
	return grpc.Dial(r.Address, grpc.WithInsecure()) //nolint:all
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
		spqrlog.Zero.Debug().
			Str("endpoint", coordinatorEndpoint).
			Msg("dialing coordinator on")

		internalR := &topology.Router{
			Address: coordinatorEndpoint,
		}
		cc, err := DialCoordinator(internalR)
		if err != nil {
			return err
		}

		if routerID == "" {
			routerID, err = randomHex(6)

			if err != nil {
				return err
			}
		}

		rCl := protos.NewRouterServiceClient(cc)
		if resp, err := rCl.AddRouter(context.Background(), &protos.AddRouterRequest{
			Router: &protos.Router{
				Id:      routerID,
				Address: routerEndpoint,
				Status:  protos.RouterStatus_OPENED,
			},
		}); err == nil {
			fmt.Printf("-------------------------------------\n")
			fmt.Printf("create router with id: %s\n", resp.Id)
			fmt.Printf("-------------------------------------\n")
		} else {
			spqrlog.Zero.Error().Err(err).Msg("")
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
		spqrlog.Zero.Debug().
			Str("endpoint", coordinatorEndpoint).
			Msg("dialing coordinator on")

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
			spqrlog.Zero.Error().Err(err).Msg("")
		}
		return nil
	},
	SilenceUsage:  false,
	SilenceErrors: false,
}

var addShardCmd = &cobra.Command{
	Use:   "AddShard",
	Short: "list running routers in current topology",
	RunE: func(cmd *cobra.Command, args []string) error {
		spqrlog.Zero.Debug().
			Str("endpoint", coordinatorEndpoint).
			Msg("dialing coordinator on")
		internalR := &topology.Router{
			Address: coordinatorEndpoint,
		}

		cc, err := DialCoordinator(internalR)
		if err != nil {
			return err
		}

		if shardID == "" {
			shardID, err = randomHex(6)

			if err != nil {
				return err
			}
		}

		rCl := protos.NewShardServiceClient(cc)
		if _, err := rCl.AddDataShard(context.Background(), &protos.AddShardRequest{
			Shard: &protos.Shard{
				Id:    shardID,
				Hosts: shardHosts,
			},
		}); err == nil {
			fmt.Printf("-------------------------------------\n")
			fmt.Printf("create shard with id: %s and hosts: %+v\n", shardID, shardHosts)
			fmt.Printf("-------------------------------------\n")
		} else {
			spqrlog.Zero.Error().Err(err).Msg("")
		}
		return nil
	},
	SilenceUsage:  false,
	SilenceErrors: false,
}

var listShardCmd = &cobra.Command{
	Use:   "ListShards",
	Short: "list running routers in current topology",
	RunE: func(cmd *cobra.Command, args []string) error {
		spqrlog.Zero.Debug().
			Str("endpoint", coordinatorEndpoint).
			Msg("dialing coordinator on")

		cc, err := DialCoordinator(&topology.Router{
			Address: coordinatorEndpoint,
		})
		if err != nil {
			return err
		}

		rCl := protos.NewShardServiceClient(cc)
		if resp, err := rCl.ListShards(context.Background(), &protos.ListShardsRequest{}); err == nil {
			fmt.Printf("-------------------------------------\n")
			fmt.Printf("%d shards found\n", len(resp.Shards))

			for _, shard := range resp.Shards {
				fmt.Printf("router %s serving on host group %+v\n", shard.Id, shard.Hosts)
			}

			fmt.Printf("-------------------------------------\n")
		} else {
			spqrlog.Zero.Error().Err(err).Msg("")
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

	addShardCmd.PersistentFlags().StringSliceVarP(&shardHosts, "shard-hosts", "", nil, "shard hosts")
	addShardCmd.PersistentFlags().StringVarP(&shardID, "shard-id", "", "", "shard id to add")

	/* --- Router cmds --- */
	rootCmd.AddCommand(listRouterCmd)
	rootCmd.AddCommand(addRouterCmd)
	/* ------------------- */
	/* --- Shard cmds --- */
	rootCmd.AddCommand(listShardCmd)
	rootCmd.AddCommand(addShardCmd)
	/* ------------------ */
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		spqrlog.Zero.Err(err).Msg("")
	}
}
