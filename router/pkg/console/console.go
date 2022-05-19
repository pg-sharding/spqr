package console

import "C"
import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/jackc/pgproto3/v2"
	"github.com/pg-sharding/spqr/pkg/clientinteractor"
	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/pkg/spqrlog"

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
	qlogger qlog.Qlog

	stchan chan struct{}
}

var _ Console = &Local{}

func (l *Local) Shutdown() error {
	return nil
}

func NewConsole(cfg *tls.Config, qrouter qrouter.QueryRouter, rrouter rrouter.RequestRouter, stchan chan struct{}) (*Local, error) {
	return &Local{
		Qrouter: qrouter,
		RRouter: rrouter,
		qlogger: qlogprovider.NewLocalQlog(),
		cfg:     cfg,
		stchan:  stchan,
	}, nil
}

type TopoCntl interface {
	kr.KeyRangeMgr
	shrule.ShardingRulesMgr
	datashards.ShardsMgr
}

func (l *Local) processQueryInternal(cli clientinteractor.PSQLInteractor, ctx context.Context, cl client.Client, q string) error {
	tstmt, err := spqrparser.Parse(q)
	if err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG1, "RouterConfig '%s', parsed %T", q, tstmt)

	switch stmt := tstmt.(type) {
	case *spqrparser.MoveKeyRange:
		spqrlog.Logger.Printf(spqrlog.DEBUG2, "parsed move %s to %s", stmt.KeyRangeID, stmt.DestShardID)
		move := &kr.MoveKeyRange{
			ShardId: stmt.DestShardID,
			Krid:    stmt.KeyRangeID,
		}
		err := l.Qrouter.Move(ctx, move)
		if err != nil {
			return cli.ReportError(err, cl)
		}
		_ = l.qlogger.DumpQuery(ctx, config.RouterConfig().AutoConf, q)
		return cli.MoveKeyRange(ctx, move, cl)

	case *spqrparser.Show:
		spqrlog.Logger.Printf(spqrlog.DEBUG2, "parsed %s", stmt.Cmd)
		switch stmt.Cmd {
		case spqrparser.ShowPoolsStr:
			//return l.Qrouter.Pools(cl)
		case spqrparser.ShowDatabasesStr:
			//return cli.Databases(t) t.Databases(cl)
		case spqrparser.ShowShardsStr:
			return cli.Shards(ctx, l.Qrouter.ListDataShards(ctx), cl)
		case spqrparser.ShowKeyRangesStr:
			if krs, err := l.Qrouter.ListKeyRange(ctx); err != nil {
				return cli.ReportError(err, cl)
			} else {
				return cli.KeyRanges(krs, cl)
			}
		case spqrparser.ShowShardingColumns:
			rules, err := l.Qrouter.ListShardingRules(ctx)
			if err != nil {
				return cli.ReportError(err, cl)
			}
			return cli.ShardingRules(ctx, rules, cl)
		default:
			spqrlog.Logger.Printf(spqrlog.DEBUG1, "Unknown default %s", stmt.Cmd)
			return fmt.Errorf("Unknown show statement: " + stmt.Cmd)
		}
		return nil
	case *spqrparser.SplitKeyRange:
		split := &kr.SplitKeyRange{
			Bound:    stmt.Border,
			Krid:     stmt.KeyRangeID,
			SourceID: stmt.KeyRangeFromID,
		}
		err := l.Qrouter.Split(ctx, split)
		if err != nil {
			return cli.ReportError(err, cl)
		}
		_ = l.qlogger.DumpQuery(ctx, config.RouterConfig().AutoConf, q)
		return cli.SplitKeyRange(ctx, split, cl)
	case *spqrparser.Lock:
		_, err := l.Qrouter.Lock(ctx, stmt.KeyRangeID)
		if err != nil {
			return cli.ReportError(err, cl)
		}
		_ = l.qlogger.DumpQuery(ctx, config.RouterConfig().AutoConf, q)
		return cli.LockKeyRange(ctx, stmt.KeyRangeID, cl)
	case *spqrparser.Unlock:
		err := l.Qrouter.Unlock(ctx, stmt.KeyRangeID)
		if err != nil {
			return cli.ReportError(err, cl)
		}
		_ = l.qlogger.DumpQuery(ctx, config.RouterConfig().AutoConf, q)
		return cli.UnlockKeyRange(ctx, stmt.KeyRangeID, cl)
	case *spqrparser.ShardingColumn:
		err := l.Qrouter.AddShardingRule(ctx, shrule.NewShardingRule([]string{stmt.ColName}))
		if err != nil {
			return cli.ReportError(err, cl)
		}
		_ = l.qlogger.DumpQuery(ctx, config.RouterConfig().AutoConf, q)
		return cli.AddShardingRule(ctx, shrule.NewShardingRule([]string{stmt.ColName}), cl)
	case *spqrparser.AddKeyRange:
		err := l.Qrouter.AddKeyRange(ctx, kr.KeyRangeFromSQL(stmt))
		if err != nil {
			return cli.ReportError(err, cl)
		}
		_ = l.qlogger.DumpQuery(ctx, config.RouterConfig().AutoConf, q)
		return cli.AddKeyRange(ctx, kr.KeyRangeFromSQL(stmt), cl)
	case *spqrparser.Shard:
		err := l.Qrouter.AddDataShard(ctx, &datashards.DataShard{
			ID: stmt.Name,
		})
		if err != nil {
			return cli.ReportError(err, cl)
		}
		_ = l.qlogger.DumpQuery(ctx, config.RouterConfig().AutoConf, q)
		return err
	case *spqrparser.Shutdown:
		return l.Shutdown()
	default:
		spqrlog.Logger.Printf(spqrlog.ERROR, "got unexpected console request %v %T", tstmt, tstmt)
		if err := cl.DefaultReply(); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
		return nil
	}
}

func (l *Local) ProcessQuery(ctx context.Context, q string, cl client.Client) error {
	return l.processQueryInternal(clientinteractor.PSQLInteractor{}, ctx, cl, q)
}

const greeting = `
		SQPR router admin console
	Here you can configure your routing rules
------------------------------------------------
	You can find documentation here 
https://github.com/pg-sharding/spqr/tree/master/doc/router
`

func (l *Local) Serve(ctx context.Context, cl client.Client) error {

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.AuthenticationOk{},
		&pgproto3.ParameterStatus{Name: "integer_datetimes", Value: "on"},
		&pgproto3.ParameterStatus{Name: "server_version", Value: "console"},
		&pgproto3.NoticeResponse{
			Message: greeting,
		},
		&pgproto3.ReadyForQuery{
			TxStatus: byte(conn.TXIDLE),
		},
	} {
		if err := cl.Send(msg); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	spqrlog.Logger.Printf(spqrlog.LOG, "console.ProcClient start")

	for {
		msg, err := cl.Receive()

		if err != nil {
			return err
		}

		switch v := msg.(type) {
		case *pgproto3.Query:
			if err := l.ProcessQuery(ctx, v.String, cl); err != nil {
				_ = cl.ReplyErrMsg(err.Error())
				// continue to consume input
			}
		default:
			spqrlog.Logger.Printf(spqrlog.ERROR, "got unexpected postgresql proto message with type %T", v)
		}
	}
}

func (l *Local) Qlog() qlog.Qlog {
	return l.qlogger
}
