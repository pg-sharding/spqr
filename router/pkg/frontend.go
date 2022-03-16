package pkg

import (
	"fmt"
	"io"

	"github.com/jackc/pgproto3/v2"
	"github.com/wal-g/tracelog"

	"github.com/pg-sharding/spqr/pkg/asynctracelog"
	"github.com/pg-sharding/spqr/router/pkg/client"
	"github.com/pg-sharding/spqr/router/pkg/qrouter"
	"github.com/pg-sharding/spqr/router/pkg/rrouter"
)

type Qinteractor interface {
}

type QinteractorImpl struct {
}

func Frontend(qr qrouter.QueryRouter, cl client.RouterClient, cmngr rrouter.ConnManager) error {

	asynctracelog.Printf("process Frontend for user %s %s", cl.Usr(), cl.DB())

	_ = cl.ReplyNotice(fmt.Sprintf("process Frontend for user %s %s", cl.Usr(), cl.DB()))

	rst := rrouter.NewRelayState(qr, cl, cmngr)

	for {
		msg, err := cl.Receive()
		if err != nil {
			if err == io.EOF {
				return nil
			}

			asynctracelog.Printf("failed to receive msg %w", err)
			return err
		}

		asynctracelog.Printf("received msg %v", msg)

		switch q := msg.(type) {
		case *pgproto3.Query:

			rst.AddQuery(*q)

			// txactive == 0 || activeSh == nil
			if cmngr.ValidateReRoute(rst) {
				switch err := rst.Reroute(q); err {
				case rrouter.SkipQueryError:
					_ = cl.ReplyNotice(fmt.Sprintf("skip executing this query, wait for next"))
					_ = cl.Reply("ok")
					continue
				case qrouter.MatchShardError:
					_ = cl.Reply(fmt.Sprintf("failed to match any datashard"))
					continue
				case qrouter.ParseError:
					_ = cl.ReplyNotice(fmt.Sprintf("skip executing this query, wait for next"))
					_ = cl.Reply("ok")
					continue
				case nil:

				default:
					tracelog.InfoLogger.Printf("encounter %w", err)
					_ = rst.UnRouteWithError(nil, err)
					return err
				}
			}

			var txst byte
			var err error
			if txst, err = rst.RelayStep(); err != nil {
				if rst.ShouldRetry(err) {
					//ch := make(chan interface{})
					//
					//status := qdb.KRUnLocked
					//_ = rst.Qr.Subscribe(rst.TargetKeyRange.ID, &status, ch)
					//<-ch
					//// retry on master
					//
					//shrds, err := rst.Reroute(q)
					//
					//if err != nil {
					//	return err
					//}
					//
					//if err := rst.Connect(shrds); err != nil {
					//	return err
					//}
					//
					//rst.ReplayBuff()
				}
				return err
			}

			if err := rst.CompleteRelay(txst); err != nil {
				return err
			}

			asynctracelog.Printf("active shards are %v", rst.ActiveShards)

		default:
			asynctracelog.Printf("received msg type %T", q)
		}
	}
}
