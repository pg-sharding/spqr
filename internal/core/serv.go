package core

import (
	"crypto/tls"
	"encoding/binary"
	"net"

	"github.com/jackc/pgproto3"
	"github.com/spqr/util"
)

type ShServer struct {
	rule *BERule
	conn net.Conn
	fr   *pgproto3.Frontend
}

func (srv *ShServer) initConn(sm *pgproto3.StartupMessage) error {

	var err error
	srv.fr, err = pgproto3.NewFrontend(pgproto3.NewChunkReader(srv.conn), srv.conn)
	if err != nil {
		return err
	}

	err = srv.fr.Send(sm)
	if err != nil {
		util.Fatal(err)
		return err
	}

	for {
		////tracelog.InfoLogger.Println("round inner")
		msg, err := srv.fr.Receive()
		if err != nil {
			util.Fatal(err)
			return err
		}
		//tracelog.InfoLogger.Println(reflect.TypeOf(msg))
		//tracelog.InfoLogger.Println(msg)
		//fatal(backend.Send(msg))
		switch v := msg.(type) {
		case *pgproto3.ReadyForQuery:
			////tracelog.InfoLogger.Println("inner ok")
			return nil

			//!! backend authBackend
		case *pgproto3.Authentication:
			err := authBackend(srv, v)
			if err != nil {
				return err
			}
		}
	}

}

func (srv *ShServer) Send(query pgproto3.FrontendMessage) error {
	return srv.fr.Send(query)
}

func (srv *ShServer) Receive() (pgproto3.BackendMessage, error) {
	return srv.fr.Receive()
}

func NewServer(rule *BERule, conn net.Conn) *ShServer {
	return &ShServer{
		rule: rule,
		conn: conn,
	}
}

func (srv *ShServer) ReqBackendSsl(cfg *tls.Config) error {

	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, 8)
	b = append(b, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(b[4:], sslproto)

	_, err := srv.conn.Write(b)

	if err != nil {
		panic(err)
	}

	resp := make([]byte, 1)

	if _, err := srv.conn.Read(resp); err != nil {
		return err
	}

	//fmt.Printf("%v", resp)

	sym := resp[0]

	//fmt.Printf("%v\n", sym)

	if sym != 'S' {
		panic("SSL SHOUD BE ENABLED")
	}

	//tracelog.InfoLogger.Printf("%v %v\n", srv.rule.TLSCfg.TLSSertPath, srv.rule.TLSCfg.ServPath)

	srv.conn = tls.Client(srv.conn, cfg)

	return nil
}

func (srv *ShServer) Cleanup() error {

	if srv.rule.PoolRollback {
		if err := srv.Send(&pgproto3.Query{
			String: "ROLLBACK",
		}); err != nil {
			return err
		}
	}

	if srv.rule.PoolDiscard {
		if err := srv.Send(&pgproto3.Query{
			String: "DISCARD ALL",
		}); err != nil {
			return err
		}
	}

	return nil
}
