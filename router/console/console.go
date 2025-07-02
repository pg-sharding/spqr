package console

import (
	"context"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/catalog"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/coord"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/pg-sharding/spqr/pkg/workloadlog"
	rclient "github.com/pg-sharding/spqr/router/client"
	"github.com/pg-sharding/spqr/router/qlog"
	qlogprovider "github.com/pg-sharding/spqr/router/qlog/provider"
	"github.com/pg-sharding/spqr/router/rulerouter"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
	"google.golang.org/grpc"
)

const greeting = `
		SPQR router admin console
	Here you can configure your routing rules
------------------------------------------------
	You can find documentation here 
https://github.com/pg-sharding/spqr/tree/master/docs
`

type Console interface {
	Serve(ctx context.Context, rc rclient.RouterClient) error
	ProcessQuery(ctx context.Context, q string, rc rclient.RouterClient, gc catalog.GrantChecker) error
	Qlog() qlog.Qlog
	Mgr() meta.EntityMgr
}

type LocalInstanceConsole struct {
	entityMgr meta.EntityMgr
	rrouter   rulerouter.RuleRouter
	qlogger   qlog.Qlog
	writer    workloadlog.WorkloadLog

	stchan chan struct{}
}

var _ Console = &LocalInstanceConsole{}

func (l *LocalInstanceConsole) Mgr() meta.EntityMgr {
	return l.entityMgr
}

func NewLocalInstanceConsole(mgr meta.EntityMgr, rrouter rulerouter.RuleRouter, stchan chan struct{}, writer workloadlog.WorkloadLog) (Console, error) { // add writer class
	return &LocalInstanceConsole{
		entityMgr: mgr,
		rrouter:   rrouter,
		qlogger:   qlogprovider.NewLocalQlog(),
		stchan:    stchan,
		writer:    writer,
	}, nil
}

// TODO : unit tests
func (l *LocalInstanceConsole) ProcessQuery(ctx context.Context, q string, rc rclient.RouterClient, gc catalog.GrantChecker) error {
	tstmt, err := spqrparser.Parse(q)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	spqrlog.Zero.Debug().
		Str("query", q).
		Type("type", tstmt).
		Msg("processQueryInternal: parsed query with type")

		/* Should we proxy this request to coordinator? */

	coordAddr, err := l.entityMgr.GetCoordinator(ctx)
	if err != nil {
		return err
	}
	if !config.RouterConfig().UseCoordinatorInit && !config.RouterConfig().WithCoordinator {
		return meta.ProcMetadataCommand(ctx, tstmt, l.entityMgr, l.rrouter, rc, l.writer, false)
	}

	mgr := l.entityMgr
	switch tstmt := tstmt.(type) {
	case *spqrparser.Show:
		if err := gc.CheckGrants(catalog.RoleAdmin, rc.Rule()); err != nil {
			return err
		}
		switch tstmt.Cmd {
		case spqrparser.RoutersStr:
			conn, err := grpc.NewClient(coordAddr, grpc.WithInsecure()) //nolint:all
			if err != nil {
				return err
			}
			defer func() {
				if err := conn.Close(); err != nil {
					spqrlog.Zero.Debug().Err(err).Msg("failed to close connection")
				}
			}()
			mgr = coord.NewAdapter(conn)
		}
	default:
		if err := gc.CheckGrants(catalog.RoleAdmin, rc.Rule()); err != nil {
			return err
		}
		conn, err := grpc.NewClient(coordAddr, grpc.WithInsecure()) //nolint:all
		if err != nil {
			return err
		}
		defer func() {
			if err := conn.Close(); err != nil {
				spqrlog.Zero.Debug().Err(err).Msg("failed to close connection")
			}
		}()
		mgr = coord.NewAdapter(conn)
	}

	spqrlog.Zero.Debug().Type("mgr type", mgr).Msg("proxy proc")
	return meta.ProcMetadataCommand(ctx, tstmt, mgr, l.rrouter, rc, l.writer, false)
}

// TODO : unit tests
func (l *LocalInstanceConsole) Serve(ctx context.Context, rc rclient.RouterClient) error {
	msgs := []pgproto3.BackendMessage{
		&pgproto3.AuthenticationOk{},
	}

	params := []string{"client_encoding", "standard_conforming_strings"}
	for _, p := range params {
		if v, ok := rc.Params()[p]; ok {
			msgs = append(msgs, &pgproto3.ParameterStatus{Name: p, Value: v})
		}
	}

	msgs = append(msgs, []pgproto3.BackendMessage{
		&pgproto3.ParameterStatus{Name: "integer_datetimes", Value: "on"},
		&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"},
		&pgproto3.ParameterStatus{Name: "DateStyle", Value: "ISO"},
		&pgproto3.ParameterStatus{Name: "server_version", Value: "console"},
		&pgproto3.NoticeResponse{
			Message: greeting,
		},
		&pgproto3.ReadyForQuery{
			TxStatus: byte(txstatus.TXIDLE),
		},
	}...)

	for _, msg := range msgs {
		if err := rc.Send(msg); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
	}

	spqrlog.Zero.Info().Msg("console.ProcClient start")

	for {
		msg, err := rc.Receive()

		if err != nil {
			return err
		}

		switch v := msg.(type) {
		case *pgproto3.Query:
			if err := l.ProcessQuery(ctx, v.String, rc, catalog.GC); err != nil {
				_ = rc.ReplyErr(err)
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
func (l *LocalInstanceConsole) Qlog() qlog.Qlog {
	return l.qlogger
}
