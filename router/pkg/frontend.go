package pkg

import (
	"fmt"

	"github.com/jackc/pgproto3"
	"github.com/pg-sharding/spqr/qdb/qdb"
	"github.com/pg-sharding/spqr/router/pkg/qrouter"
	"github.com/pg-sharding/spqr/router/pkg/rrouter"
	"github.com/wal-g/tracelog"
)

type Qinteractor interface {
}

type QinteractorImpl struct {
}

func Frontend(qr qrouter.Qrouter, cl rrouter.RouterClient, cmngr rrouter.ConnManager) error {

	tracelog.InfoLogger.Printf("process Frontend for user %s %s", cl.Usr(), cl.DB())

	_ = cl.ReplyNotice(fmt.Sprintf("process Frontend for user %s %s", cl.Usr(), cl.DB()))

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
				_ = cl.ReplyNotice(fmt.Sprintf("rerouting ypur connection"))

				shrdRoutes, err := rst.Reroute(v)

				_ = cl.ReplyNotice(fmt.Sprintf("mathed shard routes %v", shrdRoutes))

				if err != nil {
					tracelog.InfoLogger.Printf("encounter %w", err)
					_ = cl.ReplyErr(err.Error())
					_ = rst.Reset()
					continue
				}

				if err := rst.Connect(shrdRoutes); err != nil {
					tracelog.InfoLogger.Printf("encounter %w while initialing server connection", err)
					_ = rst.Reset()
					_ = cl.ReplyErr(err.Error())
					continue
				}
			}

			var txst byte
			var err error
			if txst, err = rst.RelayStep(v); err != nil {
				if rst.ShouldRetry(err) {

					ch := make(chan interface{})

					status := qdb.KRUnLocked
					_ = rst.Qr.Subscribe(rst.TargetKeyRange.ID, &status, ch)
					<-ch
					// retry on master

					shrds, err := rst.Reroute(v)

					if err != nil {
						return err
					}

					if err := rst.Connect(shrds); err != nil {
						return err
					}

					rst.ReplayBuff()

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
