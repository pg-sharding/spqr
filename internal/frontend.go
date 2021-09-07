package internal

import (
	"github.com/jackc/pgproto3"
	"github.com/pg-sharding/spqr/internal/qrouter"
	"github.com/pg-sharding/spqr/internal/rrouter"
	"github.com/wal-g/tracelog"
)

func frontend(rt qrouter.Qrouter, cl rrouter.Client, cmngr rrouter.ConnManager) error {

	tracelog.InfoLogger.Printf("process frontend for user %s %s", cl.Usr(), cl.DB())

	rst := &rrouter.RelayState{
		ActiveShards: nil,
		TxActive:     false,
	}

	for {
		msg, err := cl.Receive()
		if err != nil {
			tracelog.ErrorLogger.Printf("failed to recieve msg %w", err)
			return err
		}

		tracelog.InfoLogger.Printf("recieved msg %v", msg)

		switch v := msg.(type) {
		case *pgproto3.Query:
			// txactive == 0 || activeSh == nil
			if cmngr.ValidateReRoute(rst) {
				tracelog.InfoLogger.Printf("rerouting")

				if err := rst.Reroute(rt, cl, cmngr, v); err != nil {
					tracelog.InfoLogger.Printf("encounter %w", err)
					continue
				}
			}

			if err := rst.RelayStep(cl, cmngr); err != nil {
				return err
			}

			var txst byte
			if txst, err = cl.ProcQuery(v); err != nil {
				return err
			}

			if err := rst.CompleteRelay(cl, cmngr, txst); err != nil {
				return err
			}

			tracelog.InfoLogger.Printf("active shards are %v", rst.ActiveShards)

		default:
		}
	}
}
