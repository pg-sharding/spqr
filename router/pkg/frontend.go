package pkg

import (
	"fmt"
	"github.com/pg-sharding/spqr/router/pkg/server"
	"github.com/spaolacci/murmur3"
	"io"

	"github.com/jackc/pgproto3/v2"

	"github.com/pg-sharding/spqr/router/pkg/client"
	"github.com/pg-sharding/spqr/router/pkg/qrouter"
	"github.com/pg-sharding/spqr/router/pkg/rrouter"
	"github.com/wal-g/tracelog"
)

type Qinteractor interface {
}

type QinteractorImpl struct {
}

func Frontend(qr qrouter.QueryRouter, cl client.RouterClient, cmngr rrouter.ConnManager) error {
	tracelog.InfoLogger.Printf("process frontend for route %s %s", cl.Usr(), cl.DB())

	_ = cl.ReplyNotice(fmt.Sprintf("process frontend for route %s %s", cl.Usr(), cl.DB()))

	rst := rrouter.NewRelayState(qr, cl, cmngr)

	for {
		msg, err := cl.Receive()

		switch err {
		case io.ErrUnexpectedEOF:
			fallthrough
		case io.EOF:
			tracelog.InfoLogger.Printf("failed to receive msg, disconnect client")
			return rst.Close()
		case nil:
			// ok
		default:
			return err
		}

		tracelog.InfoLogger.Printf("received %T msg", msg)

		if !cl.Rule().PoolPreparedStatement {
			switch q := msg.(type) {
			case *pgproto3.Sync:
				rst.AddQuery(msg)
				if err := rst.ProcessMessage(true, true, cl, cmngr); err != nil {
					return err
				}
			case *pgproto3.Parse, *pgproto3.Execute, *pgproto3.Bind, *pgproto3.Describe:
				rst.AddQuery(msg)
				if err := rst.ProcessMessage(false, true, cl, cmngr); err != nil {
					return err
				}
			case *pgproto3.Query:
				tracelog.InfoLogger.Printf("received query %v", q.String)
				rst.AddQuery(msg)
				_ = rst.Parse(*q)
				if err := rst.ProcessMessage(true, true, cl, cmngr); err != nil {
					return err
				}
			default:
			}

			continue
		}

		switch q := msg.(type) {
		case *pgproto3.Sync:
			rst.AddQuery(msg)
			if err := rst.ProcessMessage(true, true, cl, cmngr); err != nil {
				return err
			}
		case *pgproto3.Parse:

			hash := murmur3.Sum64([]byte(q.Query))

			tracelog.InfoLogger.Printf(fmt.Sprintf("name %v, query %v, hash %d", q.Name, q.Query, hash))
			if err := cl.ReplyNotice(fmt.Sprintf("name %v, query %v, hash %d", q.Name, q.Query, hash)); err != nil {
				return err
			}

			//if _, ok := mp[hash]; ok {
			//	tracelog.InfoLogger.Printf("redefinition %v with hash %d", q.Query, hash)
			//}
			cl.StorePreparedStatement(q.Name, q.Query)

			// simply reply witch ok parse complete
			if err := cl.ReplyParseComplete(); err != nil {
				return err
			}

			if err := rst.PrepareStatement(hash, server.PrepStmtDesc{
				Name:  fmt.Sprintf("%d", hash),
				Query: q.Query,
			}); err != nil {
				return err
			}
		case *pgproto3.Describe:
			query := cl.PreparedStatementQueryByName(q.Name)
			hash := murmur3.Sum64([]byte(query))

			if err := rst.PrepareStatement(hash, server.PrepStmtDesc{
				Name:  fmt.Sprintf("%d", hash),
				Query: query,
			}); err != nil {
				return err
			}

			rst.AddQuery(&pgproto3.Describe{
				Name: fmt.Sprintf("%d", hash),
			})

			var txst byte
			var err error
			if txst, err = rst.RelayStep(true, true); err != nil {
				if rst.ShouldRetry(err) {
					// TODO: fix retry logic
				}
				return err
			}

			if err := rst.CompleteRelay(txst); err != nil {
				return err
			}
		case *pgproto3.Execute:
			tracelog.InfoLogger.Printf(fmt.Sprintf("simply fire parse stmt to connection"))
			rst.AddQuery(msg)
			if err := rst.ProcessMessage(false, true, cl, cmngr); err != nil {
				return err
			}
		case *pgproto3.Bind:

			query := cl.PreparedStatementQueryByName(q.PreparedStatement)

			hash := murmur3.Sum64([]byte(query))

			if err := rst.PrepareStatement(hash, server.PrepStmtDesc{
				Name:  fmt.Sprintf("%d", hash),
				Query: query,
			}); err != nil {
				return err
			}

			q.PreparedStatement = fmt.Sprintf("%d", hash)
			rst.AddQuery(q)

			var txst byte
			var err error
			if txst, err = rst.RelayStep(true, true); err != nil {
				return err
			}

			if err := rst.CompleteRelay(txst); err != nil {
				return err
			}
		case *pgproto3.Query:
			tracelog.InfoLogger.Printf("received query %v", q.String)
			rst.AddQuery(msg)
			_ = rst.Parse(*q)
			if err := rst.ProcessMessage(true, true, cl, cmngr); err != nil {
				return err
			}
		default:
		}
	}
}
