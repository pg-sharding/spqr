package frontend

import (
	"context"
	"io"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/rps"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/workloadlog"
	"github.com/pg-sharding/spqr/router/client"
	"github.com/pg-sharding/spqr/router/poolmgr"
	"github.com/pg-sharding/spqr/router/qrouter"
	"github.com/pg-sharding/spqr/router/relay"
	"github.com/pg-sharding/spqr/router/statistics"
)

// ProcessMessage: process client iteration, until next transaction status idle
func ProcessMessage(qr qrouter.QueryRouter, rst relay.RelayStateMgr, msg pgproto3.FrontendMessage) error {

	switch q := msg.(type) {
	case *pgproto3.Terminate:
		return nil
	case *pgproto3.Sync:
		statistics.RecordStartTime(statistics.StatisticsTypeRouter, time.Now(), rst.Client())

		if err := rst.ProcessExtendedBuffer(context.Background()); err != nil {
			return err
		}

		spqrlog.Zero.Debug().
			Uint("client", rst.Client().ID()).
			Msg("client connection synced")
		return nil
	case *pgproto3.Query:
		rps.OnRequest()
		statistics.RecordStartTime(statistics.StatisticsTypeRouter, time.Now(), rst.Client())

		// copy interface
		cpQ := *q
		q = &cpQ
		_, err := rst.ProcQueryAdvancedTx(q.String, func() error {
			// this call completes relay, sends RFQ
			return rst.ProcessSimpleQuery(q, true)
		}, false, true)

		return err

	/* These messages do not trigger immediate processing */
	case *pgproto3.Parse:
		// copy interface
		cpQ := *q
		q = &cpQ

		rst.AddExtendedProtocMessage(q)
		return nil
	case *pgproto3.Describe:
		// copy interface
		cpQ := *q
		q = &cpQ

		rst.AddExtendedProtocMessage(q)
		return nil
	case *pgproto3.FunctionCall:
		// copy interface
		cpQ := *q
		q = &cpQ
		spqrlog.Zero.Debug().
			Uint("client", rst.Client().ID()).
			Msg("client function call: simply fire parse stmt to connection")

		rst.AddExtendedProtocMessage(q)
		return nil
	case *pgproto3.Execute:
		// copy interface
		cpQ := *q
		q = &cpQ

		rst.AddExtendedProtocMessage(q)
		return nil
	case *pgproto3.Bind:
		// copy interface
		cpQ := *q
		q = &cpQ

		rst.AddExtendedProtocMessage(q)
		return nil

	default:
		return nil
	}
}

func Frontend(qr qrouter.QueryRouter, cl client.RouterClient, cmngr poolmgr.PoolMgr, writer workloadlog.WorkloadLog) error {
	spqrlog.Zero.Info().
		Str("user", cl.Usr()).
		Str("db", cl.DB()).
		Uint("client", spqrlog.GetPointer(cl)).
		Msg("process frontend for route")

	if config.RouterConfig().PgprotoDebug {
		_ = cl.ReplyDebugNoticef("process frontend for route %s %s", cl.Usr(), cl.DB())
	}
	rst := relay.NewRelayState(qr, cl, cmngr)

	defer func() {
		if err := rst.Close(); err != nil {
			spqrlog.Zero.Debug().Err(err).Msg("failed to close relay state")
		}
	}()

	var msg pgproto3.FrontendMessage
	var err error

	for {
		msg, err = cl.Receive()
		if err != nil {
			switch err {
			case io.ErrUnexpectedEOF:
				fallthrough
			case io.EOF:
				// EOF is OK.
				return nil
			default:
				return rst.ResetWithError(err)
			}
		}

		if writer != nil && writer.IsLogging() {
			switch writer.GetMode() {
			case workloadlog.All:
				writer.RecordWorkload(msg, cl.ID())
			case workloadlog.Client:
				if writer.ClientMatches(cl.ID()) {
					writer.RecordWorkload(msg, cl.ID())
				}
			}
		}

		err := ProcessMessage(qr, rst, msg)

		switch err {
		case nil:
			continue
		case io.ErrUnexpectedEOF:
			fallthrough
		case io.EOF:
			return nil
			// ok
		default:
			switch err.(type) {
			case *spqrerror.SpqrError:
				if rerr := rst.Client().ReplyErr(err); rerr != nil {
					return rerr
				}
			default:
				/* try to report error to user  */
				_ = rst.Client().ReplyErr(err)
				return err
			}
		}
	}
}
