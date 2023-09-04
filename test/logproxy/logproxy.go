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
	spqrlog.Zero.Debug().Msg("started parsing")
	// TODO connect to db
	startupMessage := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters: map[string]string{
			"user":     user,
			"database": db,
		},
	}
	spqrlog.Zero.Debug().Msg("created startup")
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", p.host, p.port))
	if err != nil {
		return err
	}

	frontend := pgproto3.NewFrontend(bufio.NewReader(conn), conn)

	frontend.Send(startupMessage)
	if err := frontend.Flush(); err != nil {
		return fmt.Errorf("failed to send msg to bd %w", err)
	}
	spqrlog.Zero.Debug().Msg("send startup")
	for {
		//recieve response
		retmsg, err := frontend.Receive()
		if err != nil {
			return fmt.Errorf("failed to receive msg from db %w", err)
		}

		if shouldStop(retmsg) {
			break
		}
	}

	spqrlog.Zero.Debug().Msg("connected")
	//TODO parse file (partially?)
	f, err := os.OpenFile(file, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	spqrlog.Zero.Debug().Msg("file open")

	for {
		tim, msg, err := parseFile(f)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		//TODO send requests in correct time
		fmt.Println(tim.String())
		//smth with time

		frontend.Send(msg)
		if err := frontend.Flush(); err != nil {
			return fmt.Errorf("failed to send msg to bd %w", err)
		}
		spqrlog.Zero.Debug().Msg("message sent")
		for {
			spqrlog.Zero.Debug().Msg("for")
			//recieve response
			retmsg, err := frontend.Receive()
			if err != nil {
				return fmt.Errorf("failed to receive msg from db %w", err)
			}
			fmt.Printf("wtf: %T  %+v\n", retmsg, retmsg)
			spqrlog.Zero.Debug().Msg("recieved from frontend")

			if shouldStop(retmsg) {
				spqrlog.Zero.Debug().Msg("stop")
				break
			}
			spqrlog.Zero.Debug().Msg("did not stop")
		}
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
		spqrlog.Zero.Debug().Msg("f timestamp reading")
		spqrlog.Zero.Debug().Msg(err.Error())
		return time.Now(), nil, err
	}
	var ti time.Time
	err = ti.UnmarshalBinary(timeb)
	if err != nil {
		spqrlog.Zero.Debug().Msg("f timestamp")
		spqrlog.Zero.Debug().Msg(err.Error())
		return time.Now(), nil, err
	}
	fmt.Printf("time: %s", ti.String())
	spqrlog.Zero.Debug().Msg("timestamp ok")

	//type
	tip := make([]byte, 1)

	_, err = f.Read(tip)
	if err != nil {
		return time.Now(), nil, err
	}
	if string(tip) == "X" {
		return time.Now(), nil, io.EOF
	}
	fmt.Printf("tip: %s", string(tip))
	spqrlog.Zero.Debug().Msg("tip?")

	//size
	rawSize := make([]byte, 4)

	_, err = f.Read(rawSize)
	if err != nil {
		return time.Now(), nil, err
	}

	msgSize := int(binary.BigEndian.Uint32(rawSize) - 4)
	fmt.Printf("size: %d", msgSize)
	spqrlog.Zero.Debug().Msg("size?")

	//message
	msg := make([]byte, msgSize)
	_, err = f.Read(msg)
	if err != nil {
		return time.Now(), nil, err
	}
	fmt.Printf("message: %s", string(msg))
	spqrlog.Zero.Debug().Msg("message")

	message := append(tip, rawSize...)
	message = append(message, msg...)
	spqrlog.Zero.Debug().Msg(string(message))

	fm := &pgproto3.Query{}
	err = fm.Decode(msg)
	//err = fm.Decode(message)
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
	//fmt.Printf("message type %T\n", msg)
	b2 := msg.Encode(nil)
	//spqrlog.Zero.Debug().Msg(string(b2))
	// fm := &pgproto3.Query{}
	// fmt.Printf("message type %T\n", fm)
	// er := fm.Decode(b2[5:])
	// if er != nil {
	// 	panic(er)
	// }

	t := time.Now()
	tb, err := t.MarshalBinary()
	if err != nil {
		return nil, err
	}

	compl := append(tb, b2...)

	return compl, nil
}
