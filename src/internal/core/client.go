package core

import (
	"bufio"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"github.com/jackc/pgproto3"
	"github.com/shgo/src/util"
	"github.com/wal-g/tracelog"
	"net"
)

type ShClient struct {

	conn net.Conn
	rule *Rule

	be *pgproto3.Backend

	sm *pgproto3.StartupMessage
}
//
//func (cl *ShClient) Send(msg pgproto3.FrontendMessage) error {
//	return cl.fr.Send(msg)
//}


func NewClient(pgconn net.Conn) *ShClient {

	return &ShClient{
		conn: pgconn,
	}

	//
	////pgConn, err := net.Dial("tcp6", conn.cfg.ShardMapping[i].ConnAddr)
	////util.Fatal(err)
	//
	//if ret.rule.SHStorage.ReqSSL {
	//	pgConn, err = conn.ReqBackendSsl(pgConn)
	//}
	//
	//if err != nil {
	//	return nil, err
	//}
	//
	//fr, err = pgproto3.NewFrontend(pgproto3.NewChunkReader(pgConn), pgConn)
	//util.Fatal(err)
	//
	//if err := conn.initConn(fr, conn.smFromSh(shConf), shConf); err != nil {
	//	return nil, err
	//}
}

const sslproto = 80877103

func (cl *ShClient) Init ()  error {

	var backend *pgproto3.Backend

	cr := pgproto3.NewChunkReader(bufio.NewReader(cl.conn))

	var sm *pgproto3.StartupMessage

	headerRaw, err := cr.Next(4)
	if err != nil {
		return err
	}
	msgSize := int(binary.BigEndian.Uint32(headerRaw) - 4)

	buf, err := cr.Next(msgSize)
	if err != nil {
		return err
	}

	protVer := binary.BigEndian.Uint32(buf)

	tracelog.InfoLogger.Println("prot version %v", protVer)

	if protVer == sslproto {
		_, err := cl.conn.Write([]byte{'S'})
		if err != nil {
			panic(err)
		}

		fmt.Printf("%v %v\n", cl.rule.TLSCfg.TLSSertPath, cl.rule.TLSCfg.ServPath)
		cert, err := tls.LoadX509KeyPair(cl.rule.TLSCfg.TLSSertPath, cl.rule.TLSCfg.ServPath)
		if err != nil {
			panic(err)
		}

		cfg := &tls.Config{Certificates: []tls.Certificate{cert}, InsecureSkipVerify: true}
		cl.conn = tls.Server(cl.conn, cfg)

		fmt.Printf("%v\n", cl.conn)

		backend, err = pgproto3.NewBackend(pgproto3.NewChunkReader(bufio.NewReader(cl.conn)), cl.conn)

		if err != nil {
			panic(err)
		}

		sm, err = backend.ReceiveStartupMessage()

		if err != nil {
			panic(err)
		}

	} else if protVer == pgproto3.ProtocolVersionNumber {
		// reuse
		sm = &pgproto3.StartupMessage{}
		err = sm.Decode(buf)
		if err != nil {
			util.Fatal(err)
			return err
		}

		backend, err = pgproto3.NewBackend(cr, cl.conn)
		if err != nil {
			util.Fatal(err)
			return err
		}
	}
	//!! frontend auth


	cl.sm = sm

	tracelog.InfoLogger.Println("sm prot ver %v", sm.ProtocolVersion)
	for k, v := range sm.Parameters {
		tracelog.InfoLogger.Printf("%v %v\n", k, v)
	}

	backend.Send(&pgproto3.Authentication{Type: pgproto3.AuthTypeOk})
	backend.Send(&pgproto3.ParameterStatus{Name: "integer_datetimes", Value: "on"})
	backend.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "lolkekcheburek"})
	backend.Send(&pgproto3.ReadyForQuery{})

	return nil
}

func (cl *ShClient) StartupMessage() *pgproto3.StartupMessage {
	return cl.sm
}

const defaultUsr = "default"

func (cl *ShClient) Usr() string {
	if usr, ok := cl.sm.Parameters["user"]; ok {
		return usr
	}

	return defaultUsr
}

func (cl *ShClient) DB() string {
	if db, ok := cl.sm.Parameters["dbname"]; ok {
		return db
	}

	return defaultUsr
}


func (cl * ShClient) Receive() (pgproto3.FrontendMessage, error) {
	return cl.be.Receive()
}

func (cl*ShClient) Send(msg pgproto3.BackendMessage) error {
	return cl.be.Send(msg)
}