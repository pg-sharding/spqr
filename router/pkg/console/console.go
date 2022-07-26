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

func (l *Local) processDrop(ctx context.Context, dstmt spqrparser.Statement, cli clientinteractor.PSQLInteractor, cl client.Client) error {
	switch stmt := dstmt.(type) {
	case *spqrparser.DropKeyRange:
		spqrlog.Logger.Printf(spqrlog.DEBUG2, "parsed drop %s to %s", stmt.KeyRangeID)
		err := l.Qrouter.DropKeyRange(ctx, stmt.KeyRangeID)
		if err != nil {
			return cli.ReportError(err, cl)
		}
		return cli.DropKeyRange(ctx, []string{stmt.KeyRangeID}, cl)
	case *spqrparser.DropShardingRule:
		spqrlog.Logger.Printf(spqrlog.DEBUG2, "parsed drop %s to %s", stmt.ID)
		err := l.Qrouter.DropShardingRule(ctx, stmt.ID)
		if err != nil {
			return cli.ReportError(err, cl)
		}
		return cli.DropShardingRule(ctx, stmt.ID, cl)
	default:
		return fmt.Errorf("unknown drop statement")
	}
}

func (l *Local) processQueryInternal(ctx context.Context, cli clientinteractor.PSQLInteractor, q string, cl client.Client) error {
	tstmt, err := spqrparser.Parse(q)
	if err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG1, "RouterConfig '%s', parsed %T", q, tstmt)

	switch stmt := tstmt.(type) {
	case *spqrparser.Drop:
		_ = l.qlogger.DumpQuery(ctx, config.RouterConfig().AutoConf, q)
		return l.processDrop(ctx, stmt.Element, cli, cl)
	case *spqrparser.DropAll:
		spqrlog.Logger.Printf(spqrlog.DEBUG2, "dropping all key ranges ")
		krs, err := l.Qrouter.ListKeyRange(ctx)
		if err != nil {
			return cli.ReportError(err, cl)
		}

		var ids []string
		for _, krcurr := range krs {
			ids = append(ids, krcurr.ID)
			_ = cl.ReplyNoticef("key range is goind to drop %s", krcurr.ID)
		}

		if err := l.Qrouter.DropAll(ctx); err != nil {
			return err
		}

		return cli.DropKeyRange(ctx, ids, cl)
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
	case *spqrparser.AddShardingRule:
		err := l.Qrouter.AddShardingRule(ctx, shrule.NewShardingRule(stmt.ID, stmt.ColNames))
		if err != nil {
			return cli.ReportError(err, cl)
		}
		_ = l.qlogger.DumpQuery(ctx, config.RouterConfig().AutoConf, q)
		return cli.AddShardingRule(ctx, shrule.NewShardingRule(stmt.ID, stmt.ColNames), cl)
	case *spqrparser.AddKeyRange:
		err := l.Qrouter.AddKeyRange(ctx, kr.KeyRangeFromSQL(stmt))
		if err != nil {
			return cli.ReportError(err, cl)
		}
		_ = l.qlogger.DumpQuery(ctx, config.RouterConfig().AutoConf, q)
		return cli.AddKeyRange(ctx, kr.KeyRangeFromSQL(stmt), cl)
	case *spqrparser.AddShard:
		err := l.Qrouter.AddDataShard(ctx, &datashards.DataShard{
			ID: stmt.Id,
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
	return l.processQueryInternal(ctx, clientinteractor.PSQLInteractor{}, q, cl)
}

const greeting = `
		SQPR router admin console
	Here you can configure your routing rules
------------------------------------------------
	You can find documentation here 
https://github.com/pg-sharding/spqr/tree/master/docs
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
