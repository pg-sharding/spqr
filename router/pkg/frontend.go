package pkg

import (
	"github.com/jackc/pgproto3"
	"github.com/pg-sharding/spqr/coordinator/qdb/qdb"
	"github.com/pg-sharding/spqr/router/pkg/qrouter"
	"github.com/pg-sharding/spqr/router/pkg/rrouter"
	"github.com/wal-g/tracelog"
)

func frontend(qr qrouter.Qrouter, cl rrouter.Client, cmngr rrouter.ConnManager) error {

	tracelog.InfoLogger.Printf("process frontend for user %s %s", cl.Usr(), cl.DB())

	rst := rrouter.NewRelayState(qr, cl, cmngr)

	for {
		msg, err := cl.Receive()
		if err != nil {
			tracelog.ErrorLogger.Printf("failed to recieve msg %w", err)
			return err
		}

		tracelog.InfoLogger.Printf("received msg %v", msg)

		switch v := msg.(type) {
		case *pgproto3.Query:
			// txactive == 0 || activeSh == nil
			if cmngr.ValidateReRoute(rst) {
				tracelog.InfoLogger.Printf("rerouting")

				shrdRoutes, err := rst.Reroute(v)

				if err != nil {
					tracelog.InfoLogger.Printf("encounter %w", err)
					continue
				}

				if err := rst.Connect(shrdRoutes); err != nil {
					tracelog.InfoLogger.Printf("encounter %w while initialing server connection", err)
					continue
				}
			}

			var txst byte
			var err error
			if txst, err = rst.RelayStep(cl, v); err != nil {
				if rst.ShouldRetry(err) {

					ch := make(chan interface{})

					_ = rst.Qr.Subscribe(rst.TargetKeyRange.KeyRangeID, qdb.KRUnLocked, ch)
					<-ch
					// retry on master

					shrds, err := rst.Reroute(v)

					if err != nil {
						return err
					}

					if err := rst.Connect(shrds); err != nil {
						return err
					}

					rst.ReplayBuff(cl)

				}
				return err
			}

			if err := rst.CompleteRelay(txst); err != nil {
				return err
			}

			tracelog.InfoLogger.Printf("active shards are %v", rst.ActiveShards)

		default:
		}
	}
}
