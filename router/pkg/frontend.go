package pkg

import (
	"fmt"
	"io"

	"github.com/jackc/pgproto3/v2"

	"github.com/pg-sharding/spqr/pkg/asynctracelog"
	"github.com/pg-sharding/spqr/router/pkg/client"
	"github.com/pg-sharding/spqr/router/pkg/qrouter"
	"github.com/pg-sharding/spqr/router/pkg/rrouter"
	"github.com/spaolacci/murmur3"
	"github.com/wal-g/tracelog"
)

type Qinteractor interface {
}

type QinteractorImpl struct {
}

type PrepStmtDesc struct {
	Name  string
	Query string
}

func Frontend(qr qrouter.QueryRouter, cl client.RouterClient, cmngr rrouter.ConnManager) error {
	asynctracelog.Printf("process Frontend for user %s %s", cl.Usr(), cl.DB())

	_ = cl.ReplyNotice(fmt.Sprintf("process Frontend for user %s %s", cl.Usr(), cl.DB()))

	rst := rrouter.NewRelayState(qr, cl, cmngr)

	mp := make(map[uint64]PrepStmtDesc)

	for {
		msg, err := cl.Receive()
		if err != nil {
			if err == io.EOF {
				return nil
			}

			asynctracelog.Printf("failed to receive msg %w", err)
			return err
		}

		switch q := msg.(type) {
		case *pgproto3.Parse:
			hash := murmur3.Sum64([]byte(q.Query))

			tracelog.InfoLogger.Printf(fmt.Sprintf("skip executing this query, name %v, query %v, hash %d", q.Name, q.Query, hash))
			if err := cl.ReplyNotice(fmt.Sprintf("skip executing this query, name %v, query %v, hash %d", q.Name, q.Query, hash)); err != nil {
				return err
			}

			if _, ok := mp[hash]; ok {
				tracelog.InfoLogger.Printf("redefinition %v with hash %d", q.Query, hash)
			}
			mp[hash] = PrepStmtDesc{
				Name:  q.Name,
				Query: q.Query,
			}

			// simply reply witch ok parse complete
			if err := cl.ReplyParseComplete(); err != nil {
				return err
			}
		case *pgproto3.Query:
			asynctracelog.Printf("received query %v", q.String)
			if err := rst.AddQuery(*q); err != nil {
				//return err
			}

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
					// TODO: fix retry logic
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
