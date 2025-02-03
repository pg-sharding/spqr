package frontend

import (
	"fmt"
	"io"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/workloadlog"
	"github.com/pg-sharding/spqr/router/client"
	"github.com/pg-sharding/spqr/router/poolmgr"
	"github.com/pg-sharding/spqr/router/qrouter"
	"github.com/pg-sharding/spqr/router/relay"
)

// ProcessMessage: process client iteration, until next transaction status idle
func ProcessMessage(qr qrouter.QueryRouter, rst relay.RelayStateMgr, msg pgproto3.FrontendMessage) error {

	if rst.Client().Rule().PoolMode != config.PoolModeTransaction {
		switch q := msg.(type) {
		case *pgproto3.Terminate:
			return nil
		case *pgproto3.Sync:
			// copy interface
			cpQ := *q
			q = &cpQ
			return rst.ProcessMessage(q, true, true)
		case *pgproto3.FunctionCall:
			// copy interface
			cpQ := *q
			q = &cpQ
			return rst.ProcessMessage(q, true, true)
		case *pgproto3.Parse:
			// copy interface
			cpQ := *q
			q = &cpQ
			if err := relay.ProcQueryAdvanced(rst, q.Query, func() error {
				rst.AddQuery(q)
				return rst.ProcessMessageBuf(true, true)
			}, true); err != nil {
				return err
			}

			return rst.CompleteRelay(true)
		case *pgproto3.Execute:
			// copy interface
			cpQ := *q
			q = &cpQ
			return rst.ProcessMessage(q, false, true)
		case *pgproto3.Bind:
			// copy interface
			cpQ := *q
			q = &cpQ
			return rst.ProcessMessage(q, false, true)
		case *pgproto3.Describe:
			// copy interface
			cpQ := *q
			q = &cpQ
			return rst.ProcessMessage(q, false, true)
		case *pgproto3.Query:
			// copy interface
			cpQ := *q
			q = &cpQ
			if err := relay.ProcQueryAdvanced(rst, q.String, func() error {
				rst.AddQuery(q)

				return rst.ProcessMessageBuf(true, true)
			}, false); err != nil {
				return err
			}

			return rst.CompleteRelay(true)
		default:
			return nil
		}
	}

	switch q := msg.(type) {
	case *pgproto3.Terminate:
		return nil
	case *pgproto3.Sync:
		if err := rst.ProcessExtendedBuffer(); err != nil {
			return err
		}

		spqrlog.Zero.Debug().
			Uint("client", spqrlog.GetPointer(rst.Client())).
			Msg("client connection synced")
		return nil
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
		return rst.ProcessMessage(q, false, true)
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
	case *pgproto3.Query:
		// copy interface
		cpQ := *q
		q = &cpQ
		if err := relay.ProcQueryAdvanced(rst, q.String, func() error {
			rst.AddQuery(q)
			// this call compeletes relay, sends RFQ
			return rst.ProcessMessageBuf(true, true)
		}, false); err != nil {
			return err
		}

		return rst.CompleteRelay(true)
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

	defer rst.Close()

	var msg pgproto3.FrontendMessage
	var err error

	for {
		msg, err = cl.Receive()
		if err != nil {
			switch err {
			case io.ErrUnexpectedEOF:
				fallthrough
			case io.EOF:
				return nil
				// ok
			default:
				return rst.UnRouteWithError(rst.ActiveShards(), err)
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

		if err := ProcessMessage(qr, rst, msg); err != nil {
			switch err {
			case io.ErrUnexpectedEOF:
				fallthrough
			case io.EOF:
				return nil
				// ok
			default:
				spqrlog.Zero.Error().
					Uint("client", rst.Client().ID()).Int("tx-status", int(rst.TxStatus())).Err(err).
					Msg("client iteration done with error")
				if err := rst.UnRouteWithError(rst.ActiveShards(), fmt.Errorf("client processing error: %v, tx status %s", err, rst.TxStatus().String())); err != nil {
					return err
				}
			}
		}
	}
}
