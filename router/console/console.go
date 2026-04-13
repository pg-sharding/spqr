package console

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/catalog"
	"github.com/pg-sharding/spqr/pkg/clientinteractor"
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
)

const greeting = `
   ███████╗██████╗  ██████╗ ██████╗
   ██╔════╝██╔══██╗██╔═══██╗██╔══██╗
   ███████╗██████╔╝██║   ██║██████╔╝
   ╚════██║██╔═══╝ ██║▄▄ ██║██╔══██╗
   ███████║██║     ╚██████╔╝██║  ██║
   ╚══════╝╚═╝      ╚══▀▀═╝ ╚═╝  ╚═╝

   docs: https://pg-sharding.tech
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

	displayGreeting bool

	stchan chan struct{}
}

var _ Console = &LocalInstanceConsole{}

func (l *LocalInstanceConsole) Mgr() meta.EntityMgr {
	return l.entityMgr
}

func NewLocalInstanceConsole(
	mgr meta.EntityMgr,
	rrouter rulerouter.RuleRouter,
	stchan chan struct{},
	writer workloadlog.WorkloadLog) (Console, error) { // add writer class
	return &LocalInstanceConsole{
		entityMgr:       mgr,
		rrouter:         rrouter,
		qlogger:         qlogprovider.NewLocalQlog(),
		stchan:          stchan,
		writer:          writer,
		displayGreeting: config.RouterConfig().DisplayGreeting,
	}, nil
}

func (l *LocalInstanceConsole) ExecuteMetadataQuery(
	ctx context.Context,
	tstmt spqrparser.Statement,
	rc rclient.RouterClient, gc catalog.GrantChecker) error {
	/* Should we proxy this request to coordinator? */

	mgr := l.entityMgr
	var cf func()
	var err error

	switch tstmt := tstmt.(type) {
	case *spqrparser.Show:
		if err := gc.CheckGrants(catalog.RoleAdmin, rc.Rule()); err != nil {
			return err
		}
		switch tstmt.Cmd {
		case spqrparser.RoutersStr, spqrparser.TaskGroupStr, spqrparser.TaskGroupsStr,
			spqrparser.MoveTaskStr, spqrparser.MoveTasksStr, spqrparser.SequencesStr,
			spqrparser.RedistributeTasksStr, spqrparser.TaskGroupExtendedStr, spqrparser.TaskGroupsExtendedStr:
			mgr, cf, err = coord.DistributedMgr(ctx, l.entityMgr)
			if err != nil {
				return err
			}
			defer cf()
		}
	default:
		if err := gc.CheckGrants(catalog.RoleAdmin, rc.Rule()); err != nil {
			return err
		}
		mgr, cf, err = coord.DistributedMgr(ctx, l.entityMgr)
		if err != nil {
			return err
		}
		defer cf()
	}

	spqrlog.Zero.Debug().Type("mgr type", mgr).Msg("proxy proc")
	cli := clientinteractor.NewPSQLInteractor(rc)
	tts, err := meta.ProcMetadataCommand(ctx, tstmt, mgr, l.rrouter, rc.Rule(), l.writer, false)
	if err != nil {
		return cli.ReportError(err)
	}
	return cli.ReplyTTS(tts)
}

// TODO : unit tests
func (l *LocalInstanceConsole) ProcessQuery(ctx context.Context, q string, rc rclient.RouterClient, gc catalog.GrantChecker) error {
	tstmt, err := spqrparser.Parse(q)
	if err != nil {
		spqrlog.Zero.Error().Str("query", q).Err(err).Msg("failed to parse query")
		return fmt.Errorf("failed to parse query \"%s\": %w", q, err)
	}

	spqrlog.Zero.Debug().
		Str("query", q).
		Type("type", tstmt).
		Msg("processQueryInternal: parsed query with type")

	return l.ExecuteMetadataQuery(ctx, tstmt, rc, gc)
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
		&pgproto3.BackendKeyData{ProcessID: rc.GetCancelPid(), SecretKey: rc.GetCancelKey()},
	}...)

	if l.displayGreeting {
		msgs = append(msgs,
			&pgproto3.NoticeResponse{
				Message: greeting,
			})
	}
	msgs = append(msgs,

		&pgproto3.ReadyForQuery{
			TxStatus: byte(txstatus.TXIDLE),
		},
	)

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
