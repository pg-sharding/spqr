package main

import (
	"context"
	"net"
	"os"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/pg-sharding/spqr/router/client"
	"github.com/pg-sharding/spqr/router/port"
	"github.com/pg-sharding/spqr/router/route"
)

type WorldMock struct {
	addr string
}

func NewWorldMock(addr string) *WorldMock {
	return &WorldMock{
		addr: addr,
	}
}

func (w *WorldMock) Run() error {
	ctx := context.Background()

	listener, err := net.Listen("tcp", ":6432")
	if err != nil {
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
					spqrlog.Zero.Error().Err(err).Msg("")
				}
			}()

		}
	}
}

func (w *WorldMock) serv(netconn net.Conn) error {
	cl := client.NewPsqlClient(netconn, port.DefaultRouterPortType, "", false, "")

	err := cl.Init(nil)

	if err != nil {
		return err
	}

	spqrlog.Zero.Info().
		Str("user", cl.Usr()).
		Str("db", cl.DB()).
		Msg("initialized client connection")

	if err := cl.AssignRule(&config.FrontendRule{
		AuthRule: &config.AuthCfg{
			Method: config.AuthOK,
		},
	}); err != nil {
		return err
	}

	r := route.NewRoute(nil, nil, nil)
	r.SetParams(shard.ParameterSet{})
	if err := cl.Auth(r); err != nil {
		return err
	}
	spqrlog.Zero.Info().Msg("client auth OK")

	for {
		msg, err := cl.Receive()
		if err != nil {
			return err
		}

		spqrlog.Zero.Debug().
			Interface("message", msg).
			Msg("received message")

		switch v := msg.(type) {
		case *pgproto3.Parse:
			spqrlog.Zero.Info().
				Str("name", v.Name).
				Str("query", v.Query).
				Msg("received prep stmt")
		case *pgproto3.Query:
			spqrlog.Zero.Info().
				Str("message", v.String).
				Msg("received message")

			_ = cl.ReplyDebugNotice("you are receiving the message from the mock world shard")

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
						TxStatus: byte(txstatus.TXIDLE),
					},
				} {
					if err := cl.Send(msg); err != nil {
						return err
					}
				}

				return nil
			}()

			if err != nil {
				spqrlog.Zero.Error().Err(err).Msg("")
			}

		default:
		}
	}
}
