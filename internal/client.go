package internal

import (
	"bufio"
	"crypto/tls"
	"encoding/binary"
	"net"

	"github.com/jackc/pgproto3"
	"github.com/pg-sharding/spqr/internal/config"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
)

const sslproto = 80877103 // TODO what the ?

type SpqrClient struct {
	Rule *config.FRRule
	conn net.Conn

	r *Route

	be *pgproto3.Backend

	startupMsg *pgproto3.StartupMessage
	server     Server
}

func NewClient(pgconn net.Conn) *SpqrClient {
	return &SpqrClient{
		conn:       pgconn,
		startupMsg: &pgproto3.StartupMessage{},
	}
}

func (cl *SpqrClient) Server() Server {
	return cl.server
}

func (cl *SpqrClient) Unroute() {
	cl.server = nil
}

func (cl *SpqrClient) AssignRule(rule *config.FRRule) {
	cl.Rule = rule
}

// startup + ssl
func (cl *SpqrClient) Init(cfg *tls.Config, reqssl bool) error {

	tracelog.InfoLogger.Printf("initialing client connection with %v ssl req", reqssl)

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

	//tracelog.InfoLogger.Println("prot version %v", protVer)

	if protVer == sslproto {
		_, err := cl.conn.Write([]byte{'S'})
		if err != nil {
			return err
		}

		cl.conn = tls.Server(cl.conn, cfg)

		backend, err = pgproto3.NewBackend(pgproto3.NewChunkReader(bufio.NewReader(cl.conn)), cl.conn)

		if err != nil {
			return err
		}

		sm, err = backend.ReceiveStartupMessage()

		if err != nil {
			return err
		}

	} else if protVer == pgproto3.ProtocolVersionNumber {
		// reuse
		sm = &pgproto3.StartupMessage{}
		err = sm.Decode(buf)
		tracelog.ErrorLogger.FatalOnError(err)

		backend, err = pgproto3.NewBackend(cr, cl.conn)
		tracelog.ErrorLogger.FatalOnError(err)
	}

	cl.startupMsg = sm
	cl.be = backend

	if reqssl && protVer != sslproto {
		if err := cl.Send(
			&pgproto3.ErrorResponse{
				Severity: "ERROR",
				Message:  "SSL IS REQUIRED",
			}); err != nil {
			return err
		}
	}

	return nil
}

func (cl *SpqrClient) Auth() error {
	tracelog.InfoLogger.Printf("Processing auth for %v %v\n", cl.Usr(), cl.DB())

	if err := func() error {
		switch cl.Rule.AuthRule.Method {
		case config.AuthOK:
			return nil
			// TODO:
		case config.AuthNotok:
			return errors.Errorf("user %v %v blocked", cl.Usr(), cl.DB())
		case config.AuthClearText:
			if cl.PasswordCT() != cl.Rule.AuthRule.Password {
				return errors.Errorf("user %v %v auth failed", cl.Usr(), cl.DB())
			}

			return nil
		case config.AuthMD5:

		case config.AuthScram:

		default:
			return errors.Errorf("invalid auth method %v", cl.Rule.AuthRule.Method)
		}

		return nil
	}(); err != nil {
		for _, msg := range []pgproto3.BackendMessage{
			&pgproto3.ErrorResponse{
				Message: "auth failed",
			},
		} {
			if err := cl.Send(msg); err != nil {
				return err
			}
		}
		return err
	}

	//tracelog.InfoLogger.Printf("auth client ok")

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.Authentication{Type: pgproto3.AuthTypeOk},
		&pgproto3.ParameterStatus{Name: "integer_datetimes", Value: "on"},
		&pgproto3.ParameterStatus{Name: "server_version", Value: "lolkekcheburek"},
		&pgproto3.ReadyForQuery{},
	} {
		if err := cl.Send(msg); err != nil {
			return err
		}
	}

	return nil
}

func (cl *SpqrClient) StartupMessage() *pgproto3.StartupMessage {
	return cl.startupMsg
}

const defaultUsr = "default"

func (cl *SpqrClient) Usr() string {
	if usr, ok := cl.startupMsg.Parameters["user"]; ok {
		return usr
	}

	return defaultUsr
}

func (cl *SpqrClient) DB() string {
	if db, ok := cl.startupMsg.Parameters["database"]; ok {
		return db
	}

	return defaultUsr
}

func (cl *SpqrClient) receivepasswd() string {
	msg, err := cl.be.Receive()

	if err != nil {
		return ""
	}

	switch v := msg.(type) {
	case *pgproto3.PasswordMessage:
		return v.Password
	default:
		return ""

	}
}

func (cl *SpqrClient) PasswordCT() string {
	if db, ok := cl.startupMsg.Parameters["password"]; ok {
		return db
	}

	_ = cl.be.Send(&pgproto3.Authentication{
		Type: pgproto3.AuthTypeCleartextPassword,
	})

	return cl.receivepasswd()
}

func (cl *SpqrClient) PasswordMD5() string {
	_ = cl.be.Send(&pgproto3.Authentication{
		Type: pgproto3.AuthTypeMD5Password,
		Salt: [4]byte{1, 3, 3, 7},
	})

	return cl.receivepasswd()
}

func (cl *SpqrClient) Receive() (pgproto3.FrontendMessage, error) {
	return cl.be.Receive()
}

func (cl *SpqrClient) Send(msg pgproto3.BackendMessage) error {
	return cl.be.Send(msg)
}

func (cl *SpqrClient) AssignRoute(r *Route) {
	cl.r = r
}

func (cl *SpqrClient) ProcQuery(query *pgproto3.Query) (byte, error) {

	if err := cl.server.Send(query); err != nil {
		return 0, err
	}

	for {
		msg, err := cl.server.Receive()
		if err != nil {
			return 0, err
		}
		switch v := msg.(type) {
		case *pgproto3.ReadyForQuery:
			return v.TxStatus, nil
		}

		err = cl.Send(msg)
		if err != nil {
			////tracelog.InfoLogger.Println(reflect.TypeOf(msg))
			////tracelog.InfoLogger.Println(msg)
			return 0, err
		}
	}
}

func (cl *SpqrClient) AssignServerConn(srv Server) {
	cl.server = srv
}

func (cl *SpqrClient) Route() *Route {
	return cl.r
}

func (cl *SpqrClient) DefaultReply() error {
	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.Authentication{Type: pgproto3.AuthTypeOk},
		&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
			{
				Name:                 "spqr",
				TableOID:             0,
				TableAttributeNumber: 0,
				DataTypeOID:          25,
				DataTypeSize:         -1,
				TypeModifier:         -1,
				Format:               0,
			},
		}},
		&pgproto3.DataRow{Values: [][]byte{[]byte("no data")}},
		&pgproto3.CommandComplete{CommandTag: "SELECT 1"},
		&pgproto3.ReadyForQuery{},
	} {
		if err := cl.Send(msg); err != nil {
			return err
		}
	}

	return nil
}
