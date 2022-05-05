package pkg

import (
	"fmt"
	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/router/pkg/parser"
	"github.com/pg-sharding/spqr/router/pkg/server"
	"github.com/spaolacci/murmur3"
	"io"

	"github.com/jackc/pgproto3/v2"

	"github.com/pg-sharding/spqr/router/pkg/client"
	"github.com/pg-sharding/spqr/router/pkg/qrouter"
	"github.com/pg-sharding/spqr/router/pkg/rrouter"
)

type Qinteractor interface {
}

type QinteractorImpl struct {
}

func procQuery(rst rrouter.RelayStateInteractor, q *pgproto3.Query, cmngr rrouter.ConnManager) error {
	spqrlog.Logger.Printf(spqrlog.DEBUG1, "received query %v", q.String)
	state, err := rst.Parse(q)
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
	case parser.ParseStateEmptyQuery:
		if err := rst.Client().Send(&pgproto3.EmptyQueryResponse{}); err != nil {
			return err

		}
		if err := rst.Client().Send(&pgproto3.ReadyForQuery{
			TxStatus: byte(rst.TxStatus()),
		}); err != nil {
			return err

		}
		return nil
	default:
		if err := rst.ProcessMessageBuf(true, true, cmngr); err != nil {
			return err
		}
	}
	return nil
}

func Frontend(qr qrouter.QueryRouter, cl client.RouterClient, cmngr rrouter.ConnManager) error {
	spqrlog.Logger.Printf(spqrlog.INFO, "process frontend for route %s %s", cl.Usr(), cl.DB())

	_ = cl.ReplyNotice(fmt.Sprintf("process frontend for route %s %s", cl.Usr(), cl.DB()))
	rst := rrouter.NewRelayState(qr, cl, cmngr)

	onErrFunc := func(err error) error {
		spqrlog.Logger.Errorf("frontend got error: %v", err)

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

		spqrlog.Logger.Printf(spqrlog.DEBUG1, "received %T msg, %p", msg, msg)

		if err := func() error {
			if !cl.Rule().PoolPreparedStatement {
				switch q := msg.(type) {
				case *pgproto3.Sync:
					return rst.ProcessMessage(q, true, true, cmngr)
				case *pgproto3.Parse, *pgproto3.Execute, *pgproto3.Bind, *pgproto3.Describe:
					return rst.ProcessMessage(q, false, true, cmngr)
				case *pgproto3.Query:
					return procQuery(rst, q, cmngr)
				default:
					return nil
				}
			}

			switch q := msg.(type) {
			case *pgproto3.Sync:
				return rst.ProcessMessage(q, true, true, cmngr)
			case *pgproto3.Parse:
				hash := murmur3.Sum64([]byte(q.Query))

				spqrlog.Logger.Printf(spqrlog.DEBUG1, "name %v, query %v, hash %d", q.Name, q.Query, hash)

				if err := cl.ReplyNotice(fmt.Sprintf("name %v, query %v, hash %d", q.Name, q.Query, hash)); err != nil {
					return err
				}

				cl.StorePreparedStatement(q.Name, q.Query)

				// simply reply witch ok parse complete
				return cl.ReplyParseComplete()
			case *pgproto3.Describe:
				if q.ObjectType == 'P' {
					if err := rst.ProcessMessage(q, true, true, cmngr); err != nil {
						return err
					}
					return nil
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
				}
				return err
			case *pgproto3.Execute:
				spqrlog.Logger.Printf(spqrlog.DEBUG1, "simply fire parse stmt to connection")
				return rst.ProcessMessage(q, false, true, cmngr)
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

				return rst.RelayRunCommand(q, false, true)
			case *pgproto3.Query:
				return procQuery(rst, q, cmngr)
			default:
				return nil
			}
		}(); err != nil {
			return onErrFunc(err)
		}
	}
}
