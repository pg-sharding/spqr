package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/pkg/spqrlog"

	"github.com/pg-sharding/spqr/pkg/decode"

	protos "github.com/pg-sharding/spqr/pkg/protos"
)

func Dial(addr string) (*grpc.ClientConn, error) {
	// TODO: add creds
	return grpc.Dial(addr, grpc.WithInsecure()) //nolint:all
}

var rootCmd = &cobra.Command{
	Use: "spqrdump -e localhost:7003",
	CompletionOptions: cobra.CompletionOptions{
		DisableDefaultCmd: true,
	},
	SilenceUsage:  false,
	SilenceErrors: false,
}

var endpoint string
var proto string
var passwd string
var logLevel string

// TODO : unit tests
func waitRFQ(fr *pgproto3.Frontend) error {
	for {
		if msg, err := fr.Receive(); err != nil {
			return err
		} else {
			spqrlog.Zero.Debug().
				Interface("message", msg).
				Msg("received message")
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

// TODO : unit tests
func getconn() (*pgproto3.Frontend, error) {

	cc, err := net.Dial("tcp", endpoint)
	if err != nil {
		return nil, err
	}

	frontend := pgproto3.NewFrontend(cc, cc)
	frontend.Send(&pgproto3.StartupMessage{
		ProtocolVersion: conn.SSLREQ,
	})
	if err := frontend.Flush(); err != nil {
		return nil, err
	}

	resp := make([]byte, 1)
	if _, err := cc.Read(resp); err != nil {
		return nil, err
	}

	spqrlog.Zero.Debug().
		Bytes("response", resp).
		Msg("startup got bytes")
	cc = tls.Client(cc, &tls.Config{
		InsecureSkipVerify: true,
	})

	frontend = pgproto3.NewFrontend(cc, cc)
	frontend.Send(&pgproto3.StartupMessage{
		ProtocolVersion: 196608,
		Parameters: map[string]string{
			"user":     "user1",
			"database": "spqr-console",
			"password": passwd,
		},
	})
	if err := frontend.Flush(); err != nil {
		spqrlog.Zero.Debug().
			Err(err).
			Msg("startup failed")
		return nil, err
	}

	if err := waitRFQ(frontend); err != nil {
		spqrlog.Zero.Debug().
			Err(err).
			Msg("startup failed")
		return nil, err
	}

	return frontend, nil
}

// TODO : unit tests
func DumpKeyRangesPsql() error {
	return dumpPsql("SHOW key_ranges;", func(v *pgproto3.DataRow) (string, error) {
		l := string(v.Values[2])
		r := string(v.Values[3])
		id := string(v.Values[0])
		shard := string(v.Values[1])

		return decode.KeyRange(
			&protos.KeyRangeInfo{
				KeyRange: &protos.KeyRange{LowerBound: l, UpperBound: r},
				ShardId:  shard, Krid: id}), nil
	})
}

func dumpPsql(query string, rowToStr func(v *pgproto3.DataRow) (string, error)) error {
	frontend, err := getconn()
	if err != nil {
		return err
	}
	frontend.Send(&pgproto3.Query{
		String: query,
	})
	if err := frontend.Flush(); err != nil {
		return err
	}

	for {
		if msg, err := frontend.Receive(); err != nil {
			return err
		} else {
			spqrlog.Zero.Debug().
				Interface("message", msg).
				Msg("received message")

			switch v := msg.(type) {
			case *pgproto3.DataRow:
				s, err := rowToStr(v)
				if err != nil {
					return err
				}
				fmt.Println(s)
			case *pgproto3.ErrorResponse:
				return fmt.Errorf("failed to wait for RQF: %s", v.Message)
			case *pgproto3.ReadyForQuery:
				return nil
			}
		}
	}
}

// TODO : unit tests
func DumpKeyRanges() error {
	cc, err := Dial(endpoint)
	if err != nil {
		return err
	}

	rCl := protos.NewKeyRangeServiceClient(cc)
	if keys, err := rCl.ListAllKeyRanges(context.Background(), &protos.ListAllKeyRangesRequest{}); err != nil {
		spqrlog.Zero.Error().
			Err(err).
			Msg("failed to dump endpoint rules")
	} else {
		for _, krg := range keys.KeyRangesInfo {
			fmt.Println(decode.KeyRange(krg))
		}
	}

	return nil
}

// DumpDistributions dump info about distributions & attached relations via GRPC
// TODO : unit tests
func DumpDistributions() error {
	cc, err := Dial(endpoint)
	if err != nil {
		return err
	}

	rCl := protos.NewDistributionServiceClient(cc)
	if dss, err := rCl.ListDistributions(context.Background(), &protos.ListDistributionsRequest{}); err != nil {
		spqrlog.Zero.Error().
			Err(err).
			Msg("failed to dump endpoint distributions")
	} else {
		for _, ds := range dss.Distributions {
			fmt.Println(decode.Distribution(ds))
			for _, rel := range ds.Relations {
				fmt.Println(decode.DistributedRelation(rel, ds.Id))
			}
		}
	}

	return nil
}

// DumpDistributionsPsql dump info about distributions via psql
// TODO : unit tests
func DumpDistributionsPsql() error {
	return dumpPsql("SHOW distributions;", func(v *pgproto3.DataRow) (string, error) {
		id := string(v.Values[0])
		types := string(v.Values[1])

		return decode.Distribution(
			&protos.Distribution{
				Id:          id,
				ColumnTypes: strings.Split(types, ","),
			}), nil
	})
}

// DumpRelationsPsql dump info about distributed relations via psql
// TODO : unit tests
func DumpRelationsPsql() error {
	return dumpPsql("SHOW relations;", func(v *pgproto3.DataRow) (string, error) {
		name := string(v.Values[0])
		ds := string(v.Values[1])
		dsKeyStr := strings.Split(string(v.Values[2]), ",")
		dsKey := make([]*protos.DistributionKeyEntry, len(dsKeyStr))
		for i, elem := range dsKeyStr {
			elems := strings.Split(strings.Trim(elem, "()"), ",")
			if len(elems) != 2 {
				return "", fmt.Errorf("incorrect distribution key entry: \"%s\"", elem)
			}
			dsKey[i] = &protos.DistributionKeyEntry{
				Column:       strings.Trim(elems[0], "\""),
				HashFunction: elems[1],
			}
		}

		return decode.DistributedRelation(
			&protos.DistributedRelation{
				Name:            name,
				DistributionKey: dsKey,
			}, ds), nil
	})
}

var dump = &cobra.Command{
	Use:   "dump",
	Short: "dump current sharding configuration",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := spqrlog.UpdateZeroLogLevel(logLevel); err != nil {
			return err
		}
		spqrlog.Zero.Debug().
			Str("endpoint", endpoint).
			Msg("dialing spqrdump on")

		switch proto {
		case "grpc":
			if err := DumpDistributions(); err != nil {
				return err
			}
			if err := DumpKeyRanges(); err != nil {
				return err
			}
			return nil
		case "psql":
			if err := DumpDistributionsPsql(); err != nil {
				return err
			}
			if err := DumpRelationsPsql(); err != nil {
				return err
			}
			if err := DumpKeyRangesPsql(); err != nil {
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
	rootCmd.PersistentFlags().StringVarP(&endpoint, "endpoint", "e", "localhost:7000", "endpoint for dump metadata")

	rootCmd.PersistentFlags().StringVarP(&proto, "proto", "t", "grpc", "protocol to use for communication")

	rootCmd.PersistentFlags().StringVarP(&passwd, "passwd", "p", "", "password to use for communication")

	rootCmd.PersistentFlags().StringVarP(&logLevel, "log-level", "l", "error", "log level")

	rootCmd.AddCommand(dump)
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
	}
}

func main() {
	Execute()
}
