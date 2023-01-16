package console

import "C"
import (
	"context"
	"crypto/tls"

	"github.com/pg-sharding/spqr/pkg/meta"

	"github.com/jackc/pgproto3/v2"
	"github.com/pg-sharding/spqr/pkg/clientinteractor"
	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/pkg/spqrlog"

	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/models/datashards"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/shrule"
	"github.com/pg-sharding/spqr/router/pkg/qlog"
	qlogprovider "github.com/pg-sharding/spqr/router/pkg/qlog/provider"
	"github.com/pg-sharding/spqr/router/pkg/qrouter"
	"github.com/pg-sharding/spqr/router/pkg/rulerouter"
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
	RRouter rulerouter.RuleRouter
	qlogger qlog.Qlog

	stchan chan struct{}
}

var _ Console = &Local{}

func (l *Local) Shutdown() error {
	return nil
}

func NewConsole(cfg *tls.Config, qrouter qrouter.QueryRouter, rrouter rulerouter.RuleRouter, stchan chan struct{}) (*Local, error) {
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

func (l *Local) processQueryInternal(ctx context.Context, cli *clientinteractor.PSQLInteractor, q string) error {
	tstmt, err := spqrparser.Parse(q)
	if err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG1, "RouterConfig '%s', parsed %T", q, tstmt)

	return meta.Proc(ctx, tstmt, l.Qrouter, cli)
}

func (l *Local) ProcessQuery(ctx context.Context, q string, cl client.Client) error {
	return l.processQueryInternal(ctx, clientinteractor.NewPSQLInteractor(cl), q)
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
		case *pgproto3.Terminate:
			return nil
		default:
			spqrlog.Logger.Printf(spqrlog.INFO, "got unexpected postgresql proto message with type %T", v)
		}
	}
}

func (l *Local) Qlog() qlog.Qlog {
	return l.qlogger
}
