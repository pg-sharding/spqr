package logproxy

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"os"

	"github.com/jackc/pgx/v5/pgproto3"
	con "github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
)

type Proxy struct {
	host string
	port string
}

func NewProxy(host string, port string) Proxy {
	return Proxy{
		host: host,
		port: port,
	}
}

func (p *Proxy) Run() error {
	ctx := context.Background()

	listener, err := net.Listen("tcp6", "[::1]:5433")
	if err != nil {
		spqrlog.Zero.Fatal().Err(err)
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

	spqrlog.Zero.Debug().Msg("Proxy is up and listening on port 5433")

	for {
		select {
		case <-ctx.Done():
			os.Exit(1)
		case c := <-cChan:

			go func() {
				if err := p.serv(c); err != nil {
					spqrlog.Zero.Fatal().Err(err)
				}
			}()
		}
	}
}

func (p *Proxy) serv(netconn net.Conn) error {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", p.host, p.port))
	if err != nil {
		return err
	}

	frontend := pgproto3.NewFrontend(bufio.NewReader(conn), conn)
	cl := pgproto3.NewBackend(bufio.NewReader(netconn), netconn)

	//handle startup messages
	if err = Startup(netconn, frontend, cl); err != nil {
		return err
	}

	for {
		msg, err := cl.Receive()
		if err != nil {
			return err
		}

		if err != nil {
			return fmt.Errorf("failed to receive msg from client %w", err)
		}

		//send to frontend
		frontend.Send(msg)
		if err := frontend.Flush(); err != nil {
			return fmt.Errorf("failed to send msg to bd %w", err)
		}

		for {
			//recieve response
			retmsg, err := frontend.Receive()
			if err != nil {
				return fmt.Errorf("failed to receive msg from db %w", err)
			}

			//send responce to client
			cl.Send(retmsg)
			if err := cl.Flush(); err != nil {
				return fmt.Errorf("failed to send msg to client %w", err)
			}

			if shouldStop(retmsg) {
				break
			}
		}
	}
}

func Startup(netconn net.Conn, frontend *pgproto3.Frontend, cl *pgproto3.Backend) error {
	for {
		headerRaw := make([]byte, 4)

		_, err := netconn.Read(headerRaw)
		if err != nil {
			return err
		}

		msgSize := int(binary.BigEndian.Uint32(headerRaw) - 4)
		msg := make([]byte, msgSize)

		_, err = netconn.Read(msg)
		if err != nil {
			return err
		}

		protoVer := binary.BigEndian.Uint32(msg)

		switch protoVer {
		case con.GSSREQ:
			spqrlog.Zero.Debug().Msg("negotiate gss enc request")
			_, err := netconn.Write([]byte{'N'})
			if err != nil {
				return err
			}
			// proceed next iter, for protocol version number or GSSAPI interaction
			continue

		case con.SSLREQ:
			_, err := netconn.Write([]byte{'N'})
			if err != nil {
				return err
			}
			// proceed next iter, for protocol version number or GSSAPI interaction
			continue

		case pgproto3.ProtocolVersionNumber:
			sm := &pgproto3.StartupMessage{}
			err = sm.Decode(msg)
			if err != nil {
				spqrlog.Zero.Error().Err(err)
				return err
			}

			frontend.Send(sm)
			if err := frontend.Flush(); err != nil {
				return fmt.Errorf("failed to send msg to bd %w", err)
			}
			for {
				//recieve response
				retmsg, err := frontend.Receive()
				if err != nil {
					return fmt.Errorf("failed to receive msg from db %w", err)
				}

				//send responce to client
				cl.Send(retmsg)
				if err := cl.Flush(); err != nil {
					return fmt.Errorf("failed to send msg to client %w", err)
				}

				if shouldStop(retmsg) {
					return nil
				}
			}
		default:
			return fmt.Errorf("protocol number %d not supported", protoVer)
		}
	}
}

func shouldStop(msg pgproto3.BackendMessage) bool {
	switch msg.(type) {
	case *pgproto3.ReadyForQuery:
		return true
	default:
		return false
	}
}
