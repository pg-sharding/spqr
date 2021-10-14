package main

import (
	"context"
	"net"
	"os"

	reuse "github.com/libp2p/go-reuseport"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/wal-g/tracelog"
)

type WorldMock struct {
}

func (w *WorldMock) Run() {

	ctx := context.Background()

	proto, addr := config.Get().PROTO, config.Get().Addr

	listener, err := reuse.Listen(proto, addr)
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
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

	return nil
}

func NewWorldMock() *WorldMock {
	return &WorldMock{}
}
