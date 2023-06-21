package main

import (
	"context"
	"fmt"
	"net"

	"github.com/jackc/pgproto3/v2"
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
var proto string
var passwd string

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
			fmt.Printf("%s;\n", decode.DecodeRule(rule))
		}
	}

	return nil
}

func waitRFQ(fr *pgproto3.Frontend) error {
	for {
		if msg, err := fr.Receive(); err != nil {
			return err
		} else {
			spqrlog.Logger.Printf(spqrlog.DEBUG1, "received %+v msg", msg)
			switch v := msg.(type) {
			case *pgproto3.ErrorResponse:
				if v.Severity == "ERROR" {
					return fmt.Errorf("failed to wait for RQF: %s", v.Message)
				}
			case *pgproto3.ReadyForQuery:
				return nil
			}
		}
	}
}

func DumpRulesPSQL() error {
	conn, err := net.Dial("tcp", endpoint)
	if err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	frontend := pgproto3.NewFrontend(pgproto3.NewChunkReader(conn), conn)

	if err := frontend.Send(&pgproto3.StartupMessage{
		ProtocolVersion: 196608,
		Parameters: map[string]string{
			"user":     "user1",
			"database": "spqr-console",
			"password": passwd,
		},
	}); err != nil {
		spqrlog.Logger.Printf(spqrlog.ERROR, "startup failed %v", err)
		return err
	}

	if err := waitRFQ(frontend); err != nil {
		spqrlog.Logger.Printf(spqrlog.ERROR, "startup failed %v", err)
		return err
	}

	frontend.Send(&pgproto3.Query{
		String: "SHOW key_ranges;",
	})

	for {
		if msg, err := frontend.Receive(); err != nil {
			return err
		} else {
			spqrlog.Logger.Printf(spqrlog.DEBUG1, "received %+v msg", msg)
			switch v := msg.(type) {
			case *pgproto3.DataRow:
				l := string(v.Values[2])
				r := string(v.Values[3])
				id := string(v.Values[0])
				shard := string(v.Values[1])

				fmt.Printf("%s;\n",
					decode.DecodeKeyRange(
						&protos.KeyRangeInfo{
							KeyRange: &protos.KeyRange{LowerBound: l, UpperBound: r},
							ShardId:  shard, Krid: id}))
			case *pgproto3.ErrorResponse:
				return fmt.Errorf("failed to wait for RQF: %s", v.Message)
			case *pgproto3.ReadyForQuery:
				return nil
			}
		}
	}
}

func DumpKeyRangesPSQL() error {
	conn, err := net.Dial("tcp", endpoint)
	if err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	frontend := pgproto3.NewFrontend(pgproto3.NewChunkReader(conn), conn)

	if err := frontend.Send(&pgproto3.StartupMessage{
		ProtocolVersion: 196608,
		Parameters: map[string]string{
			"user":     "user1",
			"database": "spqr-console",
			"password": passwd,
		},
	}); err != nil {
		spqrlog.Logger.Printf(spqrlog.ERROR, "startup failed %v", err)
		return err
	}

	if err := waitRFQ(frontend); err != nil {
		spqrlog.Logger.Printf(spqrlog.ERROR, "startup failed %v", err)
		return err
	}

	frontend.Send(&pgproto3.Query{
		String: "SHOW sharding_rules;",
	})

	for {
		if msg, err := frontend.Receive(); err != nil {
			return err
		} else {
			spqrlog.Logger.Printf(spqrlog.DEBUG1, "received %+v msg", msg)
			switch v := msg.(type) {
			case *pgproto3.DataRow:
				col := string(v.Values[2])
				id := string(v.Values[0])
				tablename := string(v.Values[1])

				fmt.Printf("%s;\n",
					decode.DecodeRule(
						&protos.ShardingRule{
							Id:        id,
							TableName: tablename,
							ShardingRuleEntry: []*protos.ShardingRuleEntry{
								{
									Column: col,
								},
							},
						}),
				)
			case *pgproto3.ErrorResponse:
				return fmt.Errorf("failed to wait for RQF: %s", v.Message)
			case *pgproto3.ReadyForQuery:
				return nil
			}
		}
	}
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
			fmt.Printf("%s;\n", decode.DecodeKeyRange(krg))
		}
	}

	return nil
}

var dump = &cobra.Command{
	Use:   "dump",
	Short: "list running routers in current topology",
	RunE: func(cmd *cobra.Command, args []string) error {
		spqrlog.Logger.Printf(spqrlog.INFO, "dialing coordinator on %s", endpoint)

		switch proto {
		case "grpc":
			if err := DumpRules(); err != nil {
				return err
			}
			if err := DumpKeyRanges(); err != nil {
				return err
			}
			return nil
		case "psql":
			if err := DumpRulesPSQL(); err != nil {
				return err
			}
			if err := DumpKeyRangesPSQL(); err != nil {
				return err
			}
			return nil
		default:
			return fmt.Errorf("failed to parse proto %s", proto)
		}
	},
	SilenceUsage:  false,
	SilenceErrors: false,
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&endpoint, "endpoint", "e", "localhost:7003", "endpoint for dump metadata")

	rootCmd.PersistentFlags().StringVarP(&proto, "proto", "t", "grpc", "protocol to use for communication")

	rootCmd.PersistentFlags().StringVarP(&passwd, "passwd", "p", "", "password to use for communication")

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
