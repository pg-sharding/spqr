package internal

import (
	"crypto/tls"
	"encoding/binary"
	"net"

	"github.com/jackc/pgproto3"
	"github.com/pg-sharding/spqr/internal/config"
	"github.com/pkg/errors"
)

type SpqrServer struct {
	rule     *config.BERule
	conn     net.Conn
	frontend *pgproto3.Frontend
}

func NewServer(rule *config.BERule, conn net.Conn) *SpqrServer {
	return &SpqrServer{
		rule: rule,
		conn: conn,
	}
}

func (srv *SpqrServer) initConn(sm *pgproto3.StartupMessage) error {

	var err error
	srv.frontend, err = pgproto3.NewFrontend(pgproto3.NewChunkReader(srv.conn), srv.conn)
	if err != nil {
		return err
	}

	err = srv.frontend.Send(sm)
	if err != nil {
		return err
	}

	for {
		////tracelog.InfoLogger.Println("round inner")
		msg, err := srv.frontend.Receive()
		if err != nil {
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

func (srv *SpqrServer) Send(query pgproto3.FrontendMessage) error {
	return srv.frontend.Send(query)
}

func (srv *SpqrServer) Receive() (pgproto3.BackendMessage, error) {
	return srv.frontend.Receive()
}

func (srv *SpqrServer) ReqBackendSsl(cfg *tls.Config) error {

	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, 8)
	b = append(b, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(b[4:], sslproto)

	_, err := srv.conn.Write(b)

	if err != nil {
		return errors.Wrap(err, "ReqBackendSsl")
	}

	resp := make([]byte, 1)

	if _, err := srv.conn.Read(resp); err != nil {
		return err
	}

	//tracelog.InfoLogger.Println("%v", resp)

	sym := resp[0]

	//tracelog.InfoLogger.Println("%v\n", sym)

	if sym != 'S' {
		return errors.New("SSL should be enabled")
	}

	//tracelog.InfoLogger.Printf("%v %v\n", srv.rule.TLSCfg.TLSSertPath, srv.rule.TLSCfg.ServPath)

	srv.conn = tls.Client(srv.conn, cfg)

	return nil
}

func (srv *SpqrServer) Cleanup() error {

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
