package internal

import "C"
import (
	"crypto/tls"
	"fmt"
	"net"

	"github.com/jackc/pgproto3"
	"github.com/pg-sharding/spqr/internal/config"
	"github.com/pg-sharding/spqr/internal/qrouter"
	"github.com/pg-sharding/spqr/yacc/spqrparser"
	"github.com/wal-g/tracelog"
)

type Console interface {
	Serve(netconn net.Conn) error
}

type ConsoleImpl struct {
	cfg *tls.Config
	R   qrouter.Qrouter
}

func NewConsole(cfg *tls.Config, R qrouter.Qrouter) *ConsoleImpl {
	return &ConsoleImpl{R: R, cfg: cfg}
}

func (c *ConsoleImpl) Databases(cl *SpqrClient) {
	for _, msg := range []pgproto3.BackendMessage{
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

func (c *ConsoleImpl) AddShardingColumn(cl *SpqrClient, stmt *spqrparser.ShardingColumn) {

	tracelog.InfoLogger.Printf("received create column request %s", stmt.ColName)

	err := c.R.AddColumn(stmt.ColName)

	for _, msg := range []pgproto3.BackendMessage{
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

func (c *ConsoleImpl) AddKeyRange(cl *SpqrClient, kr *spqrparser.KeyRange) {

	err := c.R.AddKeyRange(kr)

	for _, msg := range []pgproto3.BackendMessage{
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

func (c *ConsoleImpl) AddShard(cl *SpqrClient, shard *spqrparser.Shard, cfg *config.ShardCfg) {

	err := c.R.AddShard(shard.Name, cfg)

	for _, msg := range []pgproto3.BackendMessage{
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
		&pgproto3.DataRow{Values: [][]byte{[]byte(fmt.Sprintf("created shard with name %s, %w", shard.Name, err))}},
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

		if err != nil {
			return err
		}

		switch v := msg.(type) {
		case *pgproto3.Query:

			tstmt, err := spqrparser.Parse(v.String)
			if err != nil {
				return err
			}
			tracelog.InfoLogger.Printf("parsed %T", tstmt)

			switch stmt := tstmt.(type) {
			case *spqrparser.Show:

				tracelog.InfoLogger.Printf("parsed %s", stmt.Cmd)

				switch stmt.Cmd {
				case spqrparser.ShowPoolsStr: // TODO serv errors
					c.Pools(cl)
				case spqrparser.ShowDatabasesStr:
					c.Databases(cl)
				case spqrparser.ShowShardsStr:
					c.Shards(cl)
				default:
					tracelog.InfoLogger.Printf("Unknown default %s", stmt.Cmd)

					_ = cl.DefaultReply()
				}
			case *spqrparser.ShardingColumn:
				c.AddShardingColumn(cl, stmt)
			case *spqrparser.KeyRange:
				c.AddKeyRange(cl, stmt)
			case *spqrparser.Shard:
				c.AddShard(cl, stmt, &config.ShardCfg{})
			default:
				tracelog.InfoLogger.Printf("jifjweoifjwioef %v %T", tstmt, tstmt)
				if err := cl.DefaultReply(); err != nil {
					tracelog.ErrorLogger.Fatal(err)
				}
			}
		}
	}
}

func (c *ConsoleImpl) Shards(cl *SpqrClient) {

	tracelog.InfoLogger.Printf("listing shards")

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
			{
				Name:                 "spqr shards",
				TableOID:             0,
				TableAttributeNumber: 0,
				DataTypeOID:          25,
				DataTypeSize:         -1,
				TypeModifier:         -1,
				Format:               0,
			},
		},
		},
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
	if err := cl.Send(&pgproto3.DataRow{
		Values: [][]byte{[]byte(fmt.Sprintf("local node"))},
	}); err != nil {
		tracelog.InfoLogger.Print(err)
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.CommandComplete{CommandTag: "SELECT 1"},
		&pgproto3.ReadyForQuery{},
	} {
		if err := cl.Send(msg); err != nil {
			tracelog.InfoLogger.Print(err)
		}
	}
}
