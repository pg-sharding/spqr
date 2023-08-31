package logproxy

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/jackc/pgx/v5/pgproto3"
	con "github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/router/client"
)

type ProxyClient interface {
	client.PsqlClient
}

const failedToReceiveMessage = "failed to received msg %w"

func getC() (net.Conn, error) {
	const proto = "tcp"
	const addr = "[::1]:5432"
	return net.Dial(proto, addr)
}

type Proxy struct {
}

func (p *Proxy) Run() error {
	ctx := context.Background()

	listener, err := net.Listen("tcp6", "[::1]:5433")
	if err != nil {
		log.Fatal(err)
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
					log.Fatal(err)
				}
			}()
		}
	}
}

func (p *Proxy) serv(netconn net.Conn) error {

	fmt.Println("1")
	conn, err := getC()
	if err != nil {
		return err
	}

	fmt.Println("2")
	frontend := pgproto3.NewFrontend(bufio.NewReader(conn), conn)
	cl := pgproto3.NewBackend(bufio.NewReader(netconn), netconn)

	shouldStop := func(msg pgproto3.BackendMessage) bool {
		log.Printf("received msg %v", msg)

		switch msg.(type) {
		case *pgproto3.ReadyForQuery:
			return true
		default:
			return false
		}
	}

	// startupMessage := &pgproto3.StartupMessage{
	// 	ProtocolVersion: pgproto3.ProtocolVersionNumber,
	// 	Parameters: map[string]string{
	// 		"user":     "etien",
	// 		"database": "postgres",
	// 		// Add any other desired parameters
	// 	},
	// }
	// TODO client start
	if err = Startup(netconn, frontend, cl); err != nil {
		return err
	}

	// TODO END client start

	for {
		fmt.Println("4")
		msg, err := cl.Receive()
		if err != nil {
			return err
		}

		//fmt.Println(msg)

		//cb(msg)
		fmt.Println("5")

		if err != nil {
			fmt.Println(err.Error())
			continue
			//return fmt.Errorf(failedToReceiveMessage, err)
		}
		fmt.Println("6")
		frontend.Send(msg)
		if err := frontend.Flush(); err != nil {
			return fmt.Errorf(failedToReceiveMessage, err)
		}
		fmt.Println("7")
		fmt.Println("send to frontend")
		for {
			fmt.Println("8")
			retmsg, err := frontend.Receive()
			if err != nil {
				fmt.Println("error on frontend")
				fmt.Println(err)
				break
				//return fmt.Errorf(failedToReceiveMessage, err)
			}

			cl.Send(retmsg)
			if err := cl.Flush(); err != nil {
				return fmt.Errorf(failedToReceiveMessage, err)
			}

			if shouldStop(retmsg) {
				break
			}
		}
	}
}

func Startup(netconn net.Conn, frontend *pgproto3.Frontend, cl *pgproto3.Backend) error {
	shouldStop := func(msg pgproto3.BackendMessage) bool {
		log.Printf("received msg %v", msg)

		switch msg.(type) {
		case *pgproto3.ReadyForQuery:
			return true
		default:
			return false
		}
	}

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

		//
		case pgproto3.ProtocolVersionNumber:
			// reuse
			sm := &pgproto3.StartupMessage{}
			err = sm.Decode(msg)
			if err != nil {
				spqrlog.Zero.Error().Err(err).Msg("")
				return err
			}
			frontend.Send(sm)
			if err := frontend.Flush(); err != nil {
				return fmt.Errorf(failedToReceiveMessage, err)
			}
			for {
				fmt.Println("8")
				retmsg, err := frontend.Receive()
				fmt.Printf("rec: %T %+v\n", retmsg, retmsg)
				if err != nil {
					fmt.Println("error on frontend")
					fmt.Println(err)
					return fmt.Errorf(failedToReceiveMessage, err)
				}

				cl.Send(retmsg)
				if err := cl.Flush(); err != nil {
					return fmt.Errorf(failedToReceiveMessage, err)
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
