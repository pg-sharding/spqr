package frontend

import (
	"context"
	"io"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/pg-sharding/spqr/pkg/workloadlog"
	"github.com/pg-sharding/spqr/router/client"
	"github.com/pg-sharding/spqr/router/poolmgr"
	"github.com/pg-sharding/spqr/router/qrouter"
	"github.com/pg-sharding/spqr/router/relay"
	"github.com/pg-sharding/spqr/router/xproto"
)

func teardownPipeline(rst relay.RelayStateMgr, err error) error {

	if err == nil {

		if err := rst.CompleteRelay(); err != nil {
			return err
		}

		return rst.CompleteRelayClient()
	}

	if err := rst.Cleanup(); err != nil {
		return err
	}

	switch err {
	case io.ErrUnexpectedEOF:
		fallthrough
	case io.EOF:
		return err
		// ok
	default:
		spqrlog.Zero.Error().
			Uint("client", rst.Client().ID()).Int("tx-status", int(rst.QueryExecutor().TxStatus())).Err(err).
			Msg("client iteration done with error")

		/* try to report error to user  */
		if rst.QueryExecutor().TxStatus() == txstatus.TXERR {
			if rerr := rst.Client().ReplyErrWithTxStatus(err, txstatus.TXERR); rerr != nil {
				return rerr
			}
		} else {
			if rerr := rst.ResetWithError(err); rerr != nil {
				return rerr
			}
		}

		return err
	}
}

func ReplyErrUtil(rst relay.RelayStateMgr, err error) error {
	switch err {
	case nil:
		/* ok */

		return nil
	case io.ErrUnexpectedEOF:
		fallthrough
	case io.EOF:
		return err
		// ok
	default:
		spqrlog.Zero.Error().
			Uint("client", rst.Client().ID()).Int("tx-status", int(rst.QueryExecutor().TxStatus())).Err(err).
			Msg("client iteration done with error")

		/* try to report error to user  */
		if rerr := rst.Reset(); rerr != nil {
			return rerr
		}

		return rst.Client().ReplyErrMsgPure(err)
	}
}

// ProcessMessage: process client iteration, until next transaction status idle
func ProcessMessage(_ qrouter.QueryRouter, rst relay.RelayStateMgr, msg pgproto3.FrontendMessage) error {

	switch q := msg.(type) {
	case *pgproto3.Terminate:
		return nil
	case *pgproto3.Flush:
		spqrlog.Zero.Debug().
			Uint("client", rst.Client().ID()).
			Msg("client connection flushed")

		if err := ReplyErrUtil(rst, nil); err != nil {
			return err
		}

		return rst.Client().Flush()
	case *pgproto3.Sync:

		/* XXX: dont do it in flush case */
		rst.PipelineCleanup()

		spqrlog.Zero.Debug().
			Uint("client", rst.Client().ID()).
			Msg("client connection synced")

		return teardownPipeline(rst, nil)
	case *pgproto3.Close:

		return ReplyErrUtil(rst, rst.ProcessOneMsgCarefully(context.Background(), q))
	case *pgproto3.Query:

		_, err := rst.ProcQueryAdvancedTx(q.String, func() error {
			// this call completes relay, sends RFQ
			return rst.ProcessSimpleQuery(q, true)
		}, false)

		if err == nil {
			/* Okay, respond with CommandComplete first. */
			err = rst.QueryExecutor().DeriveCommandComplete()
		}

		rst.Client().ClosePreparedStatement("")

		return teardownPipeline(rst, err)
	/* These messages do not trigger immediate processing */
	case *pgproto3.Parse:
		// copy interface
		cpQ := *q
		cpQ.Query = strings.Clone(q.Query)
		cpQ.ParameterOIDs = slices.Clone(q.ParameterOIDs)

		q = &cpQ

		return ReplyErrUtil(rst, rst.ProcessOneMsgCarefully(context.Background(), q))
	case *pgproto3.Describe:

		return ReplyErrUtil(rst, rst.ProcessOneMsgCarefully(context.Background(), q))
	case *pgproto3.FunctionCall:
		// copy interface
		cpQ := *q
		q = &cpQ
		q.Arguments = xproto.CopyByteSlices(q.Arguments)

		return ReplyErrUtil(rst, rst.ProcessOneMsgCarefully(context.Background(), q))
	case *pgproto3.Execute:

		return ReplyErrUtil(rst, rst.ProcessOneMsgCarefully(context.Background(), q))

	case *pgproto3.Bind:
		// copy interface
		cpQ := *q
		q = &cpQ
		q.Parameters = xproto.CopyByteSlices(q.Parameters)
		q.ResultFormatCodes = slices.Clone(q.ResultFormatCodes)
		q.ParameterFormatCodes = slices.Clone(q.ParameterFormatCodes)

		/* Flush pending, if any */
		return ReplyErrUtil(rst, rst.ProcessOneMsgCarefully(context.Background(), q))

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
		case io.ErrUnexpectedEOF:
			// disconnect
			fallthrough
		case io.EOF:
			return nil
		}
	}
}
