package main

import (
	"context"
	"net"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/pg-sharding/spqr/router/client"
	rport "github.com/pg-sharding/spqr/router/port"
	"github.com/pg-sharding/spqr/router/route"
)

type WorldMock struct {
	addr         string
	port         string
	replyNotices bool
}

func NewWorldMock(addr string, port string) *WorldMock {
	return &WorldMock{
		addr:         addr,
		port:         port,
		replyNotices: false,
	}
}

func (w *WorldMock) Run() error {
	ctx := context.Background()

	listener, err := net.Listen("tcp", net.JoinHostPort(w.addr, w.port))
	if err != nil {
		return err
	}
	defer func() {
		if err := listener.Close(); err != nil {
			spqrlog.Zero.Debug().Err(err).Msg("failed to close listener")
		}
	}()

	spqrlog.Zero.Debug().Str("host", w.addr).Str("port", w.port).Msg("spqr worldmock listening")

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
	cl := client.NewPsqlClient(netconn, rport.DefaultRouterPortType, "", false, "")

	err := cl.Init(nil)

	if err != nil {
		return err
	}

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

	flusher := func(msgs []pgproto3.BackendMessage) error {
		for _, msg := range msgs {
			if err := cl.Send(msg); err != nil {
				return err
			}
		}

		return nil
	}

	executor := func(q string, sendDesc bool, completeRelay bool) error {

		if w.replyNotices {
			_ = cl.ReplyDebugNotice("you are receiving the message from the mock world shard")
		}

		if len(q) >= 3 && strings.ToUpper(q[0:3]) == "SET" {

			return flusher([]pgproto3.BackendMessage{
				&pgproto3.CommandComplete{CommandTag: []byte("SET")},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
			})
		} else if len(q) >= 4 && strings.ToUpper(q[0:4]) == "SHOW" {

			return flusher([]pgproto3.BackendMessage{
				&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
					{
						Name:                 []byte("pg_is_in_recovery"),
						TableOID:             0,
						TableAttributeNumber: 0,
						DataTypeOID:          25,
						DataTypeSize:         -1,
						TypeModifier:         -1,
						Format:               0,
					},
				}},
				&pgproto3.DataRow{Values: [][]byte{[]byte("off")}},
				&pgproto3.CommandComplete{CommandTag: []byte("SHOW")},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
			})
		} else {

			var msgs []pgproto3.BackendMessage
			if sendDesc {
				msgs = []pgproto3.BackendMessage{
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
				}
			} else {
				msgs = []pgproto3.BackendMessage{
					&pgproto3.DataRow{Values: [][]byte{[]byte("row1")}},
					&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
				}
			}

			if completeRelay {
				msgs = append(msgs, &pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				})
			}

			return flusher(msgs)
		}
	}

	lastQuery := ""

	for {
		msg, err := cl.Receive()
		if err != nil {
			return err
		}

		spqrlog.Zero.Debug().
			Msgf("received message: %T, %+v", msg, msg)

		switch v := msg.(type) {
		case *pgproto3.Parse:
			spqrlog.Zero.Info().
				Str("name", v.Name).
				Str("query", v.Query).
				Msg("received prep stmt")
			err := flusher([]pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},
			})

			if err != nil {
				spqrlog.Zero.Error().Err(err).Msg("error serving client")
			} else {
				lastQuery = v.Query
			}
		case *pgproto3.Bind:
			spqrlog.Zero.Info().
				Str("name", v.PreparedStatement).
				Msg("received prep stmt")
			err := flusher([]pgproto3.BackendMessage{
				&pgproto3.BindComplete{},
			})

			if err != nil {
				spqrlog.Zero.Error().Err(err).Msg("error serving client")
			}
		case *pgproto3.Execute:
			err := executor(lastQuery, true, false)

			if err != nil {
				spqrlog.Zero.Error().Err(err).Msg("error serving client")
			}
		case *pgproto3.Close:
			spqrlog.Zero.Info().
				Str("name", v.Name).
				Msg("received prep stmt")
			err := flusher([]pgproto3.BackendMessage{
				&pgproto3.CloseComplete{},
			})

			if err != nil {
				spqrlog.Zero.Error().Err(err).Msg("error serving client")
			}
		case *pgproto3.Sync:
			err = flusher([]pgproto3.BackendMessage{
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
			})

			if err != nil {
				spqrlog.Zero.Error().Err(err).Msg("error serving client")
			}
		case *pgproto3.Query:
			spqrlog.Zero.Info().
				Str("message", v.String).
				Msg("received message")

			err := executor(v.String, true, true)

			if err != nil {
				spqrlog.Zero.Error().Err(err).Msg("error serving client")
			}

		default:
		}
	}
}
