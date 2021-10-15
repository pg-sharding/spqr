package worldmock

import (
	"context"
	"net"
	"os"

	"github.com/jackc/pgproto3"
	reuse "github.com/libp2p/go-reuseport"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/router/pkg/rrouter"
	"github.com/wal-g/tracelog"
)

type WorldMock struct {
	addr string
}

func (w *WorldMock) Run() error {

	ctx := context.Background()

	proto := "tcp"

	listener, err := reuse.Listen(proto, w.addr)
	if err != nil {
		tracelog.ErrorLogger.PrintError(err)
		return err
	}
	defer listener.Close()

	cChan := make(chan net.Conn)

	accept := func(l net.Listener, cChan chan net.Conn) {
		for {
			c, err := l.Accept()
			if err != nil {
				// handle error (and then for example indicate acceptor is down)
				cChan <- nil
				return
			}
			cChan <- c
		}
	}

	go accept(listener, cChan)

	for {
		select {
		case <-ctx.Done():
			os.Exit(1)
		case conn := <-cChan:

			go func() {
				if err := w.serv(conn); err != nil {
					tracelog.ErrorLogger.PrintError(err)
				}
			}()

		}
	}
}

func (w *WorldMock) serv(conn net.Conn) error {
	cl := rrouter.NewPsqlClient(conn)

	err := cl.Init(nil, config.SSLMODEDISABLE)

	if err != nil {
		return err
	}

	tracelog.InfoLogger.Printf("initialized client connection %s-%s\n", cl.Usr(), cl.DB())

	cl.AssignRule(&config.FRRule{
		AuthRule: config.AuthRule{
			Method: config.AuthOK,
		},
	})
	if err := cl.Auth(); err != nil {
		return err
	}
	tracelog.InfoLogger.Printf("client auth OK")

	for {
		msg, err := cl.Receive()
		if err != nil {
			tracelog.ErrorLogger.Printf("failed to received msg %w", err)
			return err
		}

		tracelog.InfoLogger.Printf("received msg %v", msg)

		switch v := msg.(type) {
		case *pgproto3.Query:

			tracelog.InfoLogger.Printf("received message %v", v.String)

			_ = cl.ReplyNotice("you are receiving messagwe from mock world shard")

			err := func() error {
				for _, msg := range []pgproto3.BackendMessage{
					&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
						{
							Name:                 "worldmock",
							TableOID:             0,
							TableAttributeNumber: 0,
							DataTypeOID:          25,
							DataTypeSize:         -1,
							TypeModifier:         -1,
							Format:               0,
						},
					}},
					&pgproto3.DataRow{Values: [][]byte{[]byte("row1")}},
					&pgproto3.CommandComplete{CommandTag: "SELECT 1"},
					&pgproto3.ReadyForQuery{},
				} {
					if err := cl.Send(msg); err != nil {
						return err
					}
				}

				return nil
			}()

			if err != nil {
				tracelog.ErrorLogger.PrintError(err)
			}

		default:
		}
	}
}

func NewWorldMock(addr string) *WorldMock {
	return &WorldMock{
		addr: addr,
	}
}
