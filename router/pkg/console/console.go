package console

import "C"
import (
	"context"
	"crypto/tls"

	"github.com/jackc/pgproto3/v2"
	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/datashards"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/shrule"
	"github.com/pg-sharding/spqr/router/pkg/qlog"
	qlogprovider "github.com/pg-sharding/spqr/router/pkg/qlog/provider"
	"github.com/pg-sharding/spqr/router/pkg/qrouter"
	"github.com/pg-sharding/spqr/router/pkg/rrouter"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"golang.org/x/xerrors"
)

type Console interface {
	Serve(ctx context.Context, cl client.Client) error
	ProcessQuery(ctx context.Context, q string, cl client.Client) error
	Shutdown() error
}

type Local struct {
	cfg     *tls.Config
	Qrouter qrouter.QueryRouter
	RRouter rrouter.RequestRouter
	Qlog    qlog.Qlog

	stchan chan struct{}
}

var _ Console = &Local{}

func (c *Local) Shutdown() error {
	return nil
}

func NewConsole(cfg *tls.Config, qrouter qrouter.QueryRouter, rrouter rrouter.RequestRouter, stchan chan struct{}) (*Local, error) {
	return &Local{
		Qrouter: qrouter,
		RRouter: rrouter,
		Qlog:    qlogprovider.NewLocalQlog(),
		cfg:     cfg,
		stchan:  stchan,
	}, nil
}

type TopoCntl interface {
	kr.KeyRangeMgr
	shrule.ShardingRulesMgr
	datashards.ShardsMgr
	kr.KeyRangeMgr
}

var intf = func(qlogger qlog.Qlog, t TopoCntl, cli client.PSQLInteractor, ctx context.Context, cl client.Client, q string) error {

	tstmt, err := spqrparser.Parse(q)
	if err != nil {
		return err
	}

	tracelog.InfoLogger.Printf("RouterConfig '%s', parsed %T", q, tstmt)

	switch stmt := tstmt.(type) {
	case *spqrparser.Show:

		tracelog.InfoLogger.Printf("parsed %s", stmt.Cmd)

		switch stmt.Cmd {

		case spqrparser.ShowPoolsStr:
			//return t.Pools(cl)
		case spqrparser.ShowDatabasesStr:
			//return cli.Databases(t) t.Databases(cl)
		case spqrparser.ShowShardsStr:
			return cli.Shards(ctx, t.ListDataShards(ctx), cl)
		case spqrparser.ShowKeyRangesStr:
			if krs, err := t.ListKeyRanges(ctx); err != nil {
				return err
			} else {
				return cli.KeyRanges(krs, cl)
			}
		case spqrparser.ShowShardingColumns:
			rules, err := t.ListShardingRules(ctx)
			if err != nil {
				return err
			}
			return cli.ShardingRules(ctx, rules, cl)
		default:
			tracelog.InfoLogger.Printf("Unknown default %s", stmt.Cmd)

			return errors.New("Unknown show statement: " + stmt.Cmd)
		}
	case *spqrparser.SplitKeyRange:
		split := &kr.SplitKeyRange{
			Bound:    stmt.Border,
			Krid:     stmt.KeyRangeID,
			SourceID: stmt.KeyRangeFromID,
		}
		err := t.Split(ctx, split)
		if err != nil {
			_ = qlogger.DumpQuery(ctx, config.RouterConfig().AutoConf, q)
		}
		return cli.SplitKeyRange(ctx, split, cl)
	case *spqrparser.Lock:
		_, err := t.Lock(ctx, stmt.KeyRangeID)
		if err != nil {
			_ = qlogger.DumpQuery(ctx, config.RouterConfig().AutoConf, q)
		}
		return cli.LockKeyRange(ctx, stmt.KeyRangeID, cl)
	case *spqrparser.ShardingColumn:
		err := t.AddShardingRule(ctx, shrule.NewShardingRule([]string{stmt.ColName}))
		if err != nil {
			_ = qlogger.DumpQuery(ctx, config.RouterConfig().AutoConf, q)
		}
		return cli.AddShardingRule(ctx, shrule.NewShardingRule([]string{stmt.ColName}), cl)
	case *spqrparser.AddKeyRange:
		err := t.AddKeyRange(ctx, kr.KeyRangeFromSQL(stmt))
		if err != nil {
			_ = qlogger.DumpQuery(ctx, config.RouterConfig().AutoConf, q)
		}
		return cli.AddKeyRange(ctx, kr.KeyRangeFromSQL(stmt), cl)
	case *spqrparser.Shard:
		err := t.AddDataShard(ctx, &datashards.DataShard{
			ID: stmt.Name,
		})
		if err != nil {
			_ = qlogger.DumpQuery(ctx, config.RouterConfig().AutoConf, q)
		}
		return err
	case *spqrparser.Shutdown:
		//t.stchan <- struct{}{}
		return xerrors.New("not implemented")
	default:
		tracelog.InfoLogger.Printf("got unexcepted console request %v %T", tstmt, tstmt)
		if err := cl.DefaultReply(); err != nil {
			tracelog.ErrorLogger.Fatal(err)
		}
	}

	return nil
}

func (c *Local) ProcessQuery(ctx context.Context, q string, cl client.Client) error {
	return intf(c.Qlog, c.Qrouter, client.PSQLInteractor{}, ctx, cl, q)
}

const greeting = `
		SQPR router admin console
	Here you can configure your routing rules
------------------------------------------------
	You can find documentation here 
https://github.com/pg-sharding/spqr/tree/master/doc/router
`

func (c *Local) Serve(ctx context.Context, cl client.Client) error {

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.AuthenticationOk{},
		&pgproto3.ParameterStatus{Name: "integer_datetimes", Value: "on"},
		&pgproto3.ParameterStatus{Name: "server_version", Value: "console"},
		&pgproto3.NoticeResponse{
			Message: greeting,
		},
		&pgproto3.ReadyForQuery{},
	} {
		if err := cl.Send(msg); err != nil {
			tracelog.ErrorLogger.Fatal(err)
		}
	}

	tracelog.InfoLogger.Print("console.ProcClient start")

	for {
		msg, err := cl.Receive()

		if err != nil {
			return err
		}

		switch v := msg.(type) {
		case *pgproto3.Query:
			if err := c.ProcessQuery(ctx, v.String, cl); err != nil {
				_ = cl.ReplyErr(err.Error())
				// continue to consume input
			}
		default:
			tracelog.InfoLogger.Printf("got unexpected postgresql proto message with type %T", v)
		}
	}
}