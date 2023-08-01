package console

import (
	"context"
	"crypto/tls"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/clientinteractor"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/datashards"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/shrule"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/pg-sharding/spqr/router/qlog"
	qlogprovider "github.com/pg-sharding/spqr/router/qlog/provider"
	"github.com/pg-sharding/spqr/router/rulerouter"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

type Console interface {
	Serve(ctx context.Context, cl client.Client) error
	ProcessQuery(ctx context.Context, q string, cl client.Client) error
	Shutdown() error
}

type Local struct {
	cfg     *tls.Config
	Coord   meta.EntityMgr
	RRouter rulerouter.RuleRouter
	qlogger qlog.Qlog

	stchan chan struct{}
}

var _ Console = &Local{}

func (l *Local) Shutdown() error {
	return nil
}

func NewConsole(cfg *tls.Config, coord meta.EntityMgr, rrouter rulerouter.RuleRouter, stchan chan struct{}) (*Local, error) {
	return &Local{
		Coord:   coord,
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
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	spqrlog.Zero.Debug().
		Str("query", q).
		Type("type", tstmt).
		Msg("processQueryInternal: parsed query with type")

	return meta.Proc(ctx, tstmt, l.Coord, l.RRouter, cli)
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
	params := []string{"client_encoding", "standard_conforming_strings"}
	for _, p := range params {
		if v, ok := cl.Params()[p]; ok {
			if err := cl.Send(&pgproto3.ParameterStatus{Name: p, Value: v}); err != nil {
				spqrlog.Zero.Error().Err(err).Msg("")
				return err
			}
		}
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.AuthenticationOk{},
		&pgproto3.ParameterStatus{Name: "integer_datetimes", Value: "on"},
		&pgproto3.ParameterStatus{Name: "server_version", Value: "console"},
		&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"},
		&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"},
		&pgproto3.NoticeResponse{
			Message: greeting,
		},
		&pgproto3.ReadyForQuery{
			TxStatus: byte(txstatus.TXIDLE),
		},
	} {
		if err := cl.Send(msg); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}

	spqrlog.Zero.Info().Msg("console.ProcClient start")

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
			spqrlog.Zero.Info().
				Type("message type", v).
				Msg("got unexpected postgresql proto message with type")
		}
	}
}

func (l *Local) Qlog() qlog.Qlog {
	return l.qlogger
}
