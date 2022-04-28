package pkg

import (
	"fmt"
	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/router/pkg/parser"
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

	onErrFunc := func(err error) error {
		tracelog.InfoLogger.Printf("frontend got err %v", err)

		switch err {
		case nil:
			return nil
		case io.ErrUnexpectedEOF:
			fallthrough
		case io.EOF:
			fallthrough
			// ok
		default:
			return rst.Close()
		}
	}

	var msg pgproto3.FrontendMessage
	var err error

	for {
		msg, err = cl.Receive()
		if err != nil {
			return onErrFunc(err)
		}

		tracelog.InfoLogger.Printf("received %T msg, %p", msg, msg)

		if err := func() error {
			if !cl.Rule().PoolPreparedStatement {
				switch q := msg.(type) {
				case *pgproto3.Sync:
					if err := rst.ProcessMessage(q, true, true, cl, cmngr); err != nil {
						return err
					}
				case *pgproto3.Parse, *pgproto3.Execute, *pgproto3.Bind, *pgproto3.Describe:
					if err := rst.ProcessMessage(q, false, true, cl, cmngr); err != nil {
						return err
					}
				case *pgproto3.Query:
					tracelog.InfoLogger.Printf("received query %v", q.String)
					state, err := rst.Parse(*q)
					if err != nil {
						return err
					}
					rst.AddQuery(*q)

					switch state {
					case parser.ParseStateTXBegin:
						if err := rst.Client().Send(&pgproto3.ReadyForQuery{
							TxStatus: byte(conn.TXACT),
						}); err != nil {
							return err

						}
						rst.SetTxStatus(conn.TXACT)
						return nil
					default:
						if err := rst.ProcessMessageBuf(true, true, cl, cmngr); err != nil {
							return err
						}
					}
				default:
					return nil
				}
				return nil
			}

			switch q := msg.(type) {
			case *pgproto3.Sync:
				if err := rst.ProcessMessage(q, true, true, cl, cmngr); err != nil {
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
			case *pgproto3.Describe:
				if q.ObjectType == 'P' {
					if err := rst.ProcessMessage(q, true, true, cl, cmngr); err != nil {
						return err
					}
					break
				}
				query := cl.PreparedStatementQueryByName(q.Name)
				hash := murmur3.Sum64([]byte(query))

				if err := rst.PrepareRelayStep(cl, cmngr); err != nil {
					return err
				}

				if err := rst.PrepareStatement(hash, server.PrepStmtDesc{
					Name:  fmt.Sprintf("%d", hash),
					Query: query,
				}); err != nil {
					return err
				}

				q.Name = fmt.Sprintf("%d", hash)

				var err error
				if err = rst.RelayRunCommand(q, false, false); err != nil {
					if rst.ShouldRetry(err) {
						// TODO: fix retry logic
					}
					return err
				}

			case *pgproto3.Execute:
				tracelog.InfoLogger.Printf(fmt.Sprintf("simply fire parse stmt to connection"))

				if err := rst.ProcessMessage(q, false, true, cl, cmngr); err != nil {
					return err
				}
			case *pgproto3.Bind:
				query := cl.PreparedStatementQueryByName(q.PreparedStatement)
				hash := murmur3.Sum64([]byte(query))

				if err := rst.PrepareRelayStep(cl, cmngr); err != nil {
					return err
				}

				if err := rst.PrepareStatement(hash, server.PrepStmtDesc{
					Name:  fmt.Sprintf("%d", hash),
					Query: query,
				}); err != nil {
					return err
				}

				q.PreparedStatement = fmt.Sprintf("%d", hash)

				if err := rst.RelayRunCommand(q, false, true); err != nil {
					return err
				}

			case *pgproto3.Query:
				tracelog.InfoLogger.Printf("received query %v", q.String)
				state, err := rst.Parse(*q)
				if err != nil {
					return err
				}
				rst.AddQuery(*q)
				switch state {
				case parser.ParseStateTXBegin:
					if err := rst.Client().Send(&pgproto3.ReadyForQuery{
						TxStatus: byte(conn.TXACT),
					}); err != nil {
						return err
					}
					rst.SetTxStatus(conn.TXACT)
					return nil
				default:
					if err := rst.ProcessMessageBuf(true, true, cl, cmngr); err != nil {
						return err
					}
				}

			default:
				return nil
			}

			return nil
		}(); err != nil {
			return onErrFunc(err)
		}
	}
}
