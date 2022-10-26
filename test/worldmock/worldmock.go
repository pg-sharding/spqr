package worldmock

import (
	"context"
	"net"
	"os"

	"github.com/pg-sharding/spqr/router/pkg/datashard"
	"github.com/pg-sharding/spqr/router/pkg/route"

	"github.com/jackc/pgproto3/v2"
	"github.com/wal-g/tracelog"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/router/pkg/client"
)

type WorldMock struct {
	addr string
}

func (w *WorldMock) Run() error {

	ctx := context.Background()

	listener, err := net.Listen("tcp", ":6432")
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
		case c := <-cChan:

			go func() {
				if err := w.serv(c); err != nil {
					tracelog.ErrorLogger.PrintError(err)
				}
			}()

		}
	}
}

func (w *WorldMock) serv(netconn net.Conn) error {
	cl := client.NewPsqlClient(netconn)

	err := cl.Init(nil)

	if err != nil {
		return err
	}

	tracelog.InfoLogger.Printf("initialized client connection %s-%s\n", cl.Usr(), cl.DB())

	if err := cl.AssignRule(&config.FrontendRule{
		AuthRule: &config.AuthCfg{
			Method: config.AuthOK,
		},
	}); err != nil {
		return err
	}

	r := route.NewRoute(nil, nil, nil)
	r.SetParams(datashard.ParameterSet{})
	if err := cl.Auth(r); err != nil {
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
		case *pgproto3.Parse:
			tracelog.InfoLogger.Printf("received prep stmt %v %v", v.Name, v.Query)
			break
		case *pgproto3.Query:

			tracelog.InfoLogger.Printf("received message %v", v.String)

			_ = cl.ReplyNotice("you are receiving the message from the mock world shard")

			err := func() error {
				for _, msg := range []pgproto3.BackendMessage{
					&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
						{
							Name:                 []byte("worldmock"),
							TableOID:             0,
							TableAttributeNumber: 0,
							DataTypeOID:          25,
							DataTypeSize:         -1,
							TypeModifier:         -1,
							Format:               0,
						},
					}},
					&pgproto3.DataRow{Values: [][]byte{[]byte("row1")}},
					&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
					&pgproto3.ReadyForQuery{
						TxStatus: byte(conn.TXIDLE),
					},
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
