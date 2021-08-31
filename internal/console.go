package internal

import "C"
import (
	"crypto/tls"
	"fmt"
	"net"

	"github.com/jackc/pgproto3"
	"github.com/pg-sharding/spqr/internal/r"
	"github.com/pg-sharding/spqr/yacc/spqrparser"
	"github.com/wal-g/tracelog"
)

type Console interface {
	Serve(netconn net.Conn) error
}

type ConsoleImpl struct {
	cfg *tls.Config
	R   r.Qrouter
}

func NewConsole(cfg *tls.Config, R r.Qrouter) *ConsoleImpl {
	return &ConsoleImpl{R: R, cfg: cfg}
}

func (c *ConsoleImpl) Databases(cl *SpqrClient) {
	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.Authentication{Type: pgproto3.AuthTypeOk},
		&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
			{
				Name:                 "show dbs",
				TableOID:             0,
				TableAttributeNumber: 0,
				DataTypeOID:          25,
				DataTypeSize:         -1,
				TypeModifier:         -1,
				Format:               0,
			},
		},
		},
		&pgproto3.DataRow{Values: [][]byte{[]byte("show dbs")}},
		&pgproto3.ReadyForQuery{},
	} {
		if err := cl.Send(msg); err != nil {
			tracelog.InfoLogger.Print(err)
		}
	}
}

func (c *ConsoleImpl) Pools(cl *SpqrClient) {
	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.Authentication{Type: pgproto3.AuthTypeOk},
		&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
			{
				Name:                 "fortune",
				TableOID:             0,
				TableAttributeNumber: 0,
				DataTypeOID:          25,
				DataTypeSize:         -1,
				TypeModifier:         -1,
				Format:               0,
			},
		},
		},
		&pgproto3.DataRow{Values: [][]byte{[]byte("show pools")}},
		&pgproto3.ReadyForQuery{},
	} {
		if err := cl.Send(msg); err != nil {
			tracelog.InfoLogger.Print(err)
		}
	}
}

func (c *ConsoleImpl) AddShardingColumn(cl *SpqrClient, stmt *spqrparser.ShardingColumn, r r.Qrouter) {

	tracelog.InfoLogger.Printf("received create column request %s", stmt.ColName)

	err := r.AddColumn(stmt.ColName)

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.Authentication{Type: pgproto3.AuthTypeOk},
		&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
			{
				Name:                 "fortune",
				TableOID:             0,
				TableAttributeNumber: 0,
				DataTypeOID:          25,
				DataTypeSize:         -1,
				TypeModifier:         -1,
				Format:               0,
			},
		},
		},
		&pgproto3.DataRow{Values: [][]byte{[]byte(fmt.Sprintf("created sharding column %s, err %w", stmt.ColName, err))}},
		&pgproto3.ReadyForQuery{},
	} {
		if err := cl.Send(msg); err != nil {
			tracelog.InfoLogger.Print(err)
		}
	}
}

func (c *ConsoleImpl) AddKeyRange(cl *SpqrClient, r r.Qrouter, kr r.KeyRange) {

	err := r.AddKeyRange(kr)

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.Authentication{Type: pgproto3.AuthTypeOk},
		&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
			{
				Name:                 "fortune",
				TableOID:             0,
				TableAttributeNumber: 0,
				DataTypeOID:          25,
				DataTypeSize:         -1,
				TypeModifier:         -1,
				Format:               0,
			},
		},
		},
		&pgproto3.DataRow{Values: [][]byte{[]byte(fmt.Sprintf("created key range from %d to %d, err %w", kr.From, kr.To, err))}},
		&pgproto3.ReadyForQuery{},
	} {
		if err := cl.Send(msg); err != nil {
			tracelog.InfoLogger.Print(err)
		}
	}
}
func (c *ConsoleImpl) Serve(netconn net.Conn) error {

	cl := NewClient(netconn)
	if err := cl.Init(c.cfg, false); err != nil {
		return err
	}
	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.Authentication{Type: pgproto3.AuthTypeOk},
		&pgproto3.ParameterStatus{Name: "integer_datetimes", Value: "on"},
		&pgproto3.ParameterStatus{Name: "server_version", Value: "console"},
		&pgproto3.ReadyForQuery{},
	} {
		if err := cl.Send(msg); err != nil {
			tracelog.ErrorLogger.Fatal(err)
		}
	}

	for {
		msg, err := cl.Receive()
		tracelog.ErrorLogger.FatalOnError(err)

		switch v := msg.(type) {
		case *pgproto3.Query:

			tstmt, err := spqrparser.Parse(v.String)
			tracelog.ErrorLogger.FatalOnError(err)

			switch stmt := tstmt.(type) {
			case *spqrparser.Show:

				switch stmt.Cmd {
				case spqrparser.ShowPoolsStr: // TODO serv errors
					c.Pools(cl)
				case spqrparser.ShowDatabasesStr:
					c.Databases(cl)
				case spqrparser.ShowShards:
					c.Shards(cl)
				default:
					tracelog.InfoLogger.Printf("Unknown default %s", stmt.Cmd)

					_ = cl.DefaultReply()
				}
			case *spqrparser.ShardingColumn:

				c.AddShardingColumn(cl, stmt, c.R)
			case *spqrparser.KeyRange:
				c.AddKeyRange(cl, c.R, r.KeyRange{From: stmt.From, To: stmt.To, ShardId: stmt.ShardID})
			default:
				tracelog.InfoLogger.Printf("jifjweoifjwioef %v %T", tstmt, tstmt)
			}

			if err := cl.DefaultReply(); err != nil {
				tracelog.ErrorLogger.Fatal(err)
			}
		}
	}
}

func (c *ConsoleImpl) Shards(cl *SpqrClient) {

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.Authentication{Type: pgproto3.AuthTypeOk},
		&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
			{
				Name:                 "spqr",
				TableOID:             0,
				TableAttributeNumber: 0,
				DataTypeOID:          25,
				DataTypeSize:         -1,
				TypeModifier:         -1,
				Format:               0,
			},
		},
		},

		&pgproto3.ReadyForQuery{},
	} {
		if err := cl.Send(msg); err != nil {
			tracelog.InfoLogger.Print(err)
		}
	}

	for _, shard := range c.R.Shards() {
		if err := cl.Send(&pgproto3.DataRow{
			Values: [][]byte{[]byte(fmt.Sprintf("shard with ID %s", shard))},
		}); err != nil {
			tracelog.InfoLogger.Print(err)
		}
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.ReadyForQuery{},
	} {
		if err := cl.Send(msg); err != nil {
			tracelog.InfoLogger.Print(err)
		}
	}
}
