package app

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

type Qinteractor interface{}

type QinteractorImpl struct{}

// ProcessMessage: process client iteration, until next transaction status idle
func ProcessMessage(qr qrouter.QueryRouter, cmngr poolmgr.PoolMgr, rst relay.RelayStateMgr, msg pgproto3.FrontendMessage) error {
	ph := relay.NewSimpleProtoStateHandler(cmngr)

	if rst.Client().Rule().PoolMode != config.PoolModeTransaction {
		switch q := msg.(type) {
		case *pgproto3.Terminate:
			return nil
		case *pgproto3.Sync:
			// copy interface
			cpQ := *q
			q = &cpQ
			return rst.ProcessMessage(q, true, true, cmngr)
		case *pgproto3.FunctionCall:
			// copy interface
			cpQ := *q
			q = &cpQ
			return rst.ProcessMessage(q, true, true, cmngr)
		case *pgproto3.Parse:
			// copy interface
			cpQ := *q
			q = &cpQ
			if err := relay.ProcQueryAdvanced(rst, q.Query, ph, func() error {
				rst.AddQuery(q)
				_, err := rst.ProcessMessageBuf(true, true, false, rst.ConnMgr())
				return err
			}); err != nil {
				return err
			}

			return rst.CompleteRelay(true)
		case *pgproto3.Execute:
			// copy interface
			cpQ := *q
			q = &cpQ
			return rst.ProcessMessage(q, false, true, cmngr)
		case *pgproto3.Bind:
			// copy interface
			cpQ := *q
			q = &cpQ
			return rst.ProcessMessage(q, false, true, cmngr)
		case *pgproto3.Describe:
			// copy interface
			cpQ := *q
			q = &cpQ
			return rst.ProcessMessage(q, false, true, cmngr)
		case *pgproto3.Query:
			// copy interface
			cpQ := *q
			q = &cpQ
			if err := relay.ProcQueryAdvanced(rst, q.String, ph, func() error {
				rst.AddQuery(q)

				_, err := rst.ProcessMessageBuf(true, true, false, rst.ConnMgr())
				return err
			}); err != nil {
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
		if err := rst.ProcessExtendedBuffer(cmngr); err != nil {
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
		return rst.ProcessMessage(q, false, true, cmngr)
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
		if err := relay.ProcQueryAdvanced(rst, q.String, ph, func() error {
			rst.AddQuery(q)
			// this call compeletes relay, sends RFQ
			_, err := rst.ProcessMessageBuf(true, true, false, rst.ConnMgr())
			return err
		}); err != nil {
			return err
		}

		return rst.CompleteRelay(true)
	default:
		return nil
	}
}

func Frontend(qr qrouter.QueryRouter, cl client.RouterClient, cmngr poolmgr.PoolMgr, rcfg *config.Router, writer workloadlog.WorkloadLog) error {
	spqrlog.Zero.Info().
		Str("user", cl.Usr()).
		Str("db", cl.DB()).
		Uint("client", spqrlog.GetPointer(cl)).
		Msg("process frontend for route")

	if rcfg.PgprotoDebug {
		_ = cl.ReplyDebugNoticef("process frontend for route %s %s", cl.Usr(), cl.DB())
	}
	rst := relay.NewRelayState(qr, cl, cmngr, rcfg)

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

		if err := ProcessMessage(qr, cmngr, rst, msg); err != nil {
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
				if err := rst.UnRouteWithError(rst.ActiveShards(), fmt.Errorf("client proccessing error: %v, tx status %s", err, rst.TxStatus().String())); err != nil {
					return err
				}
			}
		}
	}
}
