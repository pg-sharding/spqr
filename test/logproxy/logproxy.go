package logproxy

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	con "github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
)

type Proxy struct {
	host            string
	port            string
	interceptedData []byte
	mut             sync.RWMutex
}

func NewProxy(host string, port string) Proxy {
	return Proxy{
		host:            host,
		port:            port,
		interceptedData: []byte{},
		mut:             sync.RWMutex{},
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
			p.Flush()
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
	if err = startup(netconn, frontend, cl); err != nil {
		return err
	}

	defer p.Flush()

	for {
		msg, err := cl.Receive()
		if err != nil {
			return fmt.Errorf("failed to receive msg from client %w", err)
		}
		fmt.Printf("message type %T\n", msg)

		//writing request data to buffer
		byt, err := encodeMessage(msg)
		if err != nil {
			return fmt.Errorf("failed to convert %w", err)
		}
		p.interceptedData = append(p.interceptedData, byt...)
		if len(p.interceptedData) > 1000000 {
			spqrlog.Zero.Debug().Msg("its soooo big")
			err = p.Flush()
			if err != nil {
				return fmt.Errorf("failed to write to file %w", err)
			}
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

func (p *Proxy) Flush() error {
	spqrlog.Zero.Debug().Msg("flush")
	f, err := os.OpenFile("mylog.txt", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(p.interceptedData)
	if err != nil {
		return err
	}

	return nil
}

func (p *Proxy) ReplayLogs(host string, port string, user string, file string, db string) error {
	startupMessage := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters: map[string]string{
			"user":     user,
			"database": db,
		},
	}
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", p.host, p.port))
	if err != nil {
		return err
	}

	frontend := pgproto3.NewFrontend(bufio.NewReader(conn), conn)

	frontend.Send(startupMessage)
	if err := frontend.Flush(); err != nil {
		return fmt.Errorf("failed to send msg to bd %w", err)
	}
	for {
		retmsg, err := frontend.Receive()
		if err != nil {
			return fmt.Errorf("failed to receive msg from db %w", err)
		}

		if shouldStop(retmsg) {
			break
		}
	}

	f, err := os.OpenFile(file, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	spqrlog.Zero.Debug().Msg("begin sending")

	var curt time.Timer
	tim, msg, err := parseFile(f)
	if err != nil {
		if err == io.EOF {
			return nil
		}
		return err
	}
	prevT := tim
	curt = *time.NewTimer(tim.Sub(tim))
	for {
		<-curt.C

		spqrlog.Zero.Debug().Any("msg %+v ", msg).Msg("sending")
		frontend.Send(msg)
		if err := frontend.Flush(); err != nil {
			return fmt.Errorf("failed to send msg to bd %w", err)
		}
		for {
			retmsg, err := frontend.Receive()
			if err != nil {
				return fmt.Errorf("failed to receive msg from db %w", err)
			}

			if shouldStop(retmsg) {
				break
			}
		}

		tim, msg, err = parseFile(f)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		spqrlog.Zero.Debug().Str("waiting for: %s", tim.Sub(prevT).String()).Msg("wait timer")
		curt = *time.NewTimer(tim.Sub(prevT))
		prevT = tim
	}
}

func parseFile(f *os.File) (time.Time, pgproto3.FrontendMessage, error) {
	// 15 byte - timestamp
	// 1 byte - message header
	// 4 bytes - message length (except header)
	// ?? bytes - message bytes

	//timestamp
	timeb := make([]byte, 15)
	_, err := f.Read(timeb) //err
	if err != nil {
		return time.Now(), nil, err
	}
	var ti time.Time
	err = ti.UnmarshalBinary(timeb)
	if err != nil {
		return time.Now(), nil, err
	}

	//header
	tip := make([]byte, 1)

	_, err = f.Read(tip)
	if err != nil {
		return time.Now(), nil, err
	}
	if string(tip) == "X" {
		return time.Now(), nil, io.EOF
	}

	//size
	rawSize := make([]byte, 4)

	_, err = f.Read(rawSize)
	if err != nil {
		return time.Now(), nil, err
	}

	msgSize := int(binary.BigEndian.Uint32(rawSize) - 4)

	//message
	msg := make([]byte, msgSize)
	_, err = f.Read(msg)
	if err != nil {
		return time.Now(), nil, err
	}

	fm := &pgproto3.Query{}
	err = fm.Decode(msg)
	if err != nil {
		return time.Now(), nil, err
	}

	return ti, fm, nil
}

func startup(netconn net.Conn, frontend *pgproto3.Frontend, cl *pgproto3.Backend) error {
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

/*
Gets pgproto3.FrontendMessage and encodes it in binary with timestamp.
15 byte - timestamp
1 byte - message header
4 bytes - message length (except header)
?? bytes - message bytes
*/
func encodeMessage(msg pgproto3.FrontendMessage) ([]byte, error) {
	b2 := msg.Encode(nil)

	t := time.Now()
	tb, err := t.MarshalBinary()
	if err != nil {
		return nil, err
	}

	compl := append(tb, b2...)
	return compl, nil
}
