package internal

import "C"
import (
	"crypto/tls"
	"errors"
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
	processQ(q string, cl Client) error
	Shutdown() error
}

type ConsoleImpl struct {
	cfg     *tls.Config
	Qrouter qrouter.Qrouter
}

var _ Console = &ConsoleImpl{}

func (c *ConsoleImpl) Shutdown() error {
	return nil
}

func NewConsole(cfg *tls.Config, Qrouter qrouter.Qrouter) *ConsoleImpl {
	return &ConsoleImpl{Qrouter: Qrouter, cfg: cfg}
}

func (c *ConsoleImpl) Databases(cl Client) error {
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

	return nil
}

func (c *ConsoleImpl) Pools(cl Client) error {
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

	return nil
}

func (c *ConsoleImpl) AddShardingColumn(cl Client, stmt *spqrparser.ShardingColumn) error {

	tracelog.InfoLogger.Printf("received create column request %s", stmt.ColName)

	err := c.Qrouter.AddColumn(stmt.ColName)

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

	return nil
}

func (c *ConsoleImpl) SplitKeyRange(cl Client, splitReq *spqrparser.SplitKeyRange) error {
	if err := c.Qrouter.Split(splitReq); err != nil {
		return err
	}

	tracelog.InfoLogger.Printf("splitted key range %v by %v", splitReq.KeyRangeID, splitReq.Border)

	for _, msg := range []pgproto3.BackendMessage{
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
		&pgproto3.DataRow{Values: [][]byte{[]byte(fmt.Sprintf("split key range with id %v", splitReq.KeyRangeID))}},
		&pgproto3.ReadyForQuery{},
	} {
		if err := cl.Send(msg); err != nil {
			tracelog.InfoLogger.Print(err)
		}
	}

	return nil
}

func (c *ConsoleImpl) LockKeyRange(cl Client, krid string) error {
	tracelog.InfoLogger.Printf("received lock key range req for id %v", krid)
	if err := c.Qrouter.Lock(krid); err != nil {
		return err
	}

	for _, msg := range []pgproto3.BackendMessage{
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
		&pgproto3.DataRow{Values: [][]byte{[]byte(fmt.Sprintf("lock key range with id %v", krid))}},
		&pgproto3.ReadyForQuery{},
	} {
		if err := cl.Send(msg); err != nil {
			tracelog.InfoLogger.Print(err)
		}
	}

	return nil
}

func (c *ConsoleImpl) AddKeyRange(cl Client, kr *spqrparser.KeyRange) error {

	tracelog.InfoLogger.Printf("received create key range request %s for shard", kr.ShardID)

	err := c.Qrouter.AddKeyRange(kr)

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

	return nil
}

func (c *ConsoleImpl) AddShard(cl Client, shard *spqrparser.Shard, cfg *config.ShardCfg) error {

	err := c.Qrouter.AddShard(shard.Name, cfg)

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

	return nil
}

func (c *ConsoleImpl) KeyRanges(cl Client) error {

	tracelog.InfoLogger.Printf("listing key ranges")

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
			return err
		}
	}

	for _, shard := range c.Qrouter.Shards() {
		if err := cl.Send(&pgproto3.DataRow{
			Values: [][]byte{[]byte(fmt.Sprintf("key range for shard with ID %s", shard))},
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

	return nil
}

func (c *ConsoleImpl) Shards(cl Client) error {

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
			return err
		}
	}

	for _, shard := range c.Qrouter.Shards() {
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

	return nil
}

func (c *ConsoleImpl) processQ(q string, cl Client) error {
	tstmt, err := spqrparser.Parse(q)
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
			return c.Databases(cl)
		case spqrparser.ShowShardsStr:
			return c.Shards(cl)
		case spqrparser.ShowKeyRangesStr:
			return c.KeyRanges(cl)
		default:
			tracelog.InfoLogger.Printf("Unknown default %s", stmt.Cmd)

			return errors.New("Unknown default cmd: " + stmt.Cmd)
		}
	case *spqrparser.SplitKeyRange:
		return c.SplitKeyRange(cl, stmt)
	case *spqrparser.Lock:
		return c.LockKeyRange(cl, stmt.KeyRangeID)
	case *spqrparser.ShardingColumn:
		return c.AddShardingColumn(cl, stmt)
	case *spqrparser.KeyRange:
		return c.AddKeyRange(cl, stmt)
	case *spqrparser.Shard:
		return c.AddShard(cl, stmt, &config.ShardCfg{})
	default:
		tracelog.InfoLogger.Printf("got unexcepted console request %v %T", tstmt, tstmt)
		if err := cl.DefaultReply(); err != nil {
			tracelog.ErrorLogger.Fatal(err)
		}
	}

	return nil
}

func (c *ConsoleImpl) Serve(netconn net.Conn) error {
	cl := NewClient(netconn)

	if err := cl.Init(c.cfg, config.SSLMODEDISABLE); err != nil {
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
			if err := c.processQ(v.String, cl); err != nil {
				cl.ReplyErr(err.Error())
				return err
			}
		}
	}
}
