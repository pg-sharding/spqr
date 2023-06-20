package main

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/pg-sharding/spqr/pkg/spqrlog"

	"github.com/pg-sharding/spqr/pkg/decode"

	protos "github.com/pg-sharding/spqr/pkg/protos"
)

func Dial(addr string) (*grpc.ClientConn, error) {
	// TODO: add creds
	return grpc.Dial(addr, grpc.WithInsecure())
}

var rootCmd = &cobra.Command{
	Use: "coorctl -e localhost:7003",
	CompletionOptions: cobra.CompletionOptions{
		DisableDefaultCmd: true,
	},
	SilenceUsage:  false,
	SilenceErrors: false,
}

var endpoint string

func DumpRules() error {
	cc, err := Dial(endpoint)
	if err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	rCl := protos.NewShardingRulesServiceClient(cc)
	if rules, err := rCl.ListShardingRules(context.Background(), &protos.ListShardingRuleRequest{}); err != nil {
		spqrlog.Logger.Errorf("failed to dump endpoint rules: %v", err)
	} else {
		for _, rule := range rules.Rules {
			fmt.Printf("%s", decode.DecodeRule(rule))
		}
	}

	return nil
}

func DumpKeyRanges() error {
	cc, err := Dial(endpoint)
	if err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	rCl := protos.NewKeyRangeServiceClient(cc)
	if keys, err := rCl.ListKeyRange(context.Background(), &protos.ListKeyRangeRequest{}); err != nil {
		spqrlog.Logger.Errorf("failed to dump endpoint rules: %v", err)
	} else {
		for _, krg := range keys.KeyRangesInfo {
			fmt.Printf("%s", decode.DecodeKeyRange(krg))
		}
	}

	return nil
}

var dump = &cobra.Command{
	Use:   "dump",
	Short: "list running routers in current topology",
	RunE: func(cmd *cobra.Command, args []string) error {
		spqrlog.Logger.Printf(spqrlog.DEBUG3, "dialing coordinator on %s", endpoint)
		if err := DumpRules(); err != nil {
			return err
		}
		if err := DumpKeyRanges(); err != nil {
			return err
		}
		return nil
	},
	SilenceUsage:  false,
	SilenceErrors: false,
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&endpoint, "endpoint", "e", "localhost:7003", "coordinator endpoint")

	rootCmd.AddCommand(dump)
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		spqrlog.Logger.FatalOnError(err)
	}
}

func main() {
	Execute()
}
