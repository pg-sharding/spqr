package logproxy

import (
	"context"
	"fmt"
	"net"
	"os"

	"github.com/jackc/pgproto3/v2"
	//"github.com/pg-sharding/spqr/pkg/config"
	//"github.com/pg-sharding/spqr/router/pkg/client"
	"github.com/wal-g/tracelog"
)

func getC() (net.Conn, error) {
	const proto = "tcp"
	const addr = "[::1]:6432"
	return net.Dial(proto, addr)
}

type Proxy struct {
}

func (p *Proxy) Run() error {
	ctx := context.Background()

	listener, err := net.Listen("tcp6", "[::1]:5433")
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
				if err := p.serv(c); err != nil {
					tracelog.ErrorLogger.PrintError(err)
				}
			}()

		}
	}
}

func (p *Proxy) serv(netconn net.Conn) error {

	conn, err := getC()
	if err != nil {
		fmt.Printf("failed %w", err)
		return err
	}

	frontend := pgproto3.NewFrontend(pgproto3.NewChunkReader(conn), conn)
	cl := pgproto3.NewBackend(pgproto3.NewChunkReader(netconn), netconn)

	//	err = cl.Init(nil, config.SSLMODEDISABLE)

	//	if err != nil {
	//		return err
	//	}

	//	tracelog.InfoLogger.Printf("initialized client connection %s-%s\n", cl.Usr(), cl.DB())

	//	if err := cl.AssignRule(&config.FRRule{
	//		AuthRule: config.AuthRule{
	//			Method: config.AuthOK,
	//		},
	//	}); err != nil {
	//		return err
	//	}

	//	if err := cl.Auth(); err != nil {
	//		return err
	//	}
	//	tracelog.InfoLogger.Printf("client auth OK")

	cb := func(msg pgproto3.FrontendMessage) {
		tracelog.InfoLogger.Printf("received msg %v", msg)

		switch v := msg.(type) {
		case *pgproto3.Parse:
			tracelog.InfoLogger.Printf("received prep stmt %v %v", v.Name, v.Query)
			break
		case *pgproto3.Query:

			tracelog.InfoLogger.Printf("received message %v", v.String)
		default:
		}
	}
	shouldStop := func(msg pgproto3.BackendMessage) bool {
		tracelog.InfoLogger.Printf("received msg %v", msg)

		switch msg.(type) {
		case *pgproto3.ReadyForQuery:
			return false
		default:
			return false
		}
	}

	for {
		msg, err := cl.Receive()

		cb(msg)

		if err != nil {
			tracelog.ErrorLogger.Printf("failed to received msg %w", err)
			return err
		}
		if err := frontend.Send(msg); err != nil {
			tracelog.ErrorLogger.Printf("failed to received msg %w", err)
			return err
		}
		for {
			retmsg, err := frontend.Receive()
			if err != nil {
				tracelog.ErrorLogger.Printf("failed to received msg %w", err)
				return err
			}

			err = cl.Send(retmsg)
			if err != nil {
				tracelog.ErrorLogger.Printf("failed to received msg %w", err)
				return err
			}

			if shouldStop(retmsg) {
				break
			}
		}
	}
}

func NewProxy() *Proxy {
	return &Proxy{}
}
