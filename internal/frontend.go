package internal

import (
	"github.com/jackc/pgproto3"
	"github.com/pg-sharding/spqr/internal/qrouter"
	"github.com/wal-g/tracelog"
)

const TXREL = 73


type interactor struct {

}



func frontend(rt *qrouter.QrouterImpl, cl Client, cmngr ConnManager) error {

	tracelog.InfoLogger.Printf("process frontend for user %s %s", cl.Usr(), cl.DB())

	msgs := make([]pgproto3.Query, 0)

	rst := &RelayState{
		ActiveShards:      nil,
		TxActive:          false,
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

			msgs = append(msgs, pgproto3.Query{
				String: v.String,
			})

			// txactive == 0 || activeSh == nil
			if cmngr.ValidateReRoute(rst) {

				shardNames := rt.Route(v.String)

				shards := []Shard{}

				for _, name := range shardNames {
					shards = append(shards, NewShard(name, rt.ShardCfgs[name]))
				}

				if shardNames == nil {
					return cmngr.UnRouteWithError(cl, nil, "failed to match shard")
				} else {
					tracelog.InfoLogger.Printf("parsed shard name %s", shardNames)
				}

				if rst.ActiveShards != nil {

					for _, shard := range rst.ActiveShards {
						if err := cmngr.UnRouteCB(cl, shard); err != nil {
							tracelog.ErrorLogger.PrintError(err)
						}
					}
				}

				rst.ActiveShards = shards

				for _, shard := range rst.ActiveShards {
					if err := cmngr.RouteCB(cl, shard); err != nil {
						tracelog.ErrorLogger.PrintError(err)
					}
				}
			}

			var txst byte
			for _, msg := range msgs {
				if txst, err = cl.ProcQuery(&msg); err != nil {
					return err
				}
			}

			msgs = make([]pgproto3.Query, 0)

			if err := cl.Send(&pgproto3.ReadyForQuery{}); err != nil {
				return err
			}

			if txst == TXREL {
				if rst.TxActive {
					if err := cmngr.TXEndCB(cl, rst); err != nil {
						return err
					}
					rst.TxActive = false
				}
			} else {
				if !rst.TxActive {
					if err := cmngr.TXBeginCB(cl, rst); err != nil {
						return err
					}
					rst.TxActive = true
				}
			}

		default:
			//msgs := append(msgs, msg)
		}
	}
}
