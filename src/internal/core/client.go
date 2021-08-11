package core

import (
	"bufio"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"net"

	"github.com/jackc/pgproto3"
	"github.com/shgo/src/util"
	"github.com/wal-g/tracelog"
	"golang.org/x/xerrors"
)

type ShClient struct {
	conn net.Conn
	rule *FRRule

	r *Route

	be *pgproto3.Backend

	sm     *pgproto3.StartupMessage
	shconn *ShServer
}

func (cl *ShClient) ShardConn() *ShServer {
	return cl.shconn
}

func (cl *ShClient) Unroute() {
	cl.shconn = nil
}

func NewClient(pgconn net.Conn) *ShClient {
	return &ShClient{
		conn: pgconn,
		sm:   &pgproto3.StartupMessage{},
	}
}

func (cl *ShClient) AssignRule(rule *FRRule) {
	cl.rule = rule
}

// startup + ssl
func (cl *ShClient) Init(tlscgf TLSConfig, reqssl bool) error {

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

	tracelog.InfoLogger.Println("prot version %v", protVer)

	if protVer == sslproto {
		_, err := cl.conn.Write([]byte{'S'})
		if err != nil {
			panic(err)
		}

		fmt.Printf("%v %v\n", tlscgf.TLSSertPath, tlscgf.ServPath)
		cert, err := tls.LoadX509KeyPair(tlscgf.TLSSertPath, tlscgf.ServPath)
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

	cl.sm = sm
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

	tracelog.InfoLogger.Println("sm prot ver %v", sm.ProtocolVersion)
	for k, v := range sm.Parameters {
		tracelog.InfoLogger.Printf("%v %v\n", k, v)
	}

	return nil
}
func (cl *ShClient) Auth() error {

	tracelog.InfoLogger.Printf("processing auth for %v %v\n", cl.Usr(), cl.DB())

	if err := func() error {
		switch cl.rule.AuthRule.Am {
		case AuthOK:
			return nil
			// TODO:
		case AuthNOTOK:
			return xerrors.Errorf("user %v %v blocked", cl.Usr(), cl.DB())
		case AuthClearText:
			if cl.PasswordCT() != cl.rule.AuthRule.Password {
				return xerrors.Errorf("user %v %v auth failed", cl.Usr(), cl.DB())
			}

			return nil
		case AuthMD5:

		case AuthSASL:

		default:
			return xerrors.Errorf("invalid auth method %v", cl.rule.AuthRule.Am)
		}

		return nil
	}(); err != nil {
		for _, msg := range []pgproto3.BackendMessage{
			&pgproto3.ErrorResponse{
				Message: "auth failed",
			},
		} {
			if err :=
				cl.Send(msg); err != nil {
				tracelog.InfoLogger.Printf("server startup resp failed %v %v\n", msg, err)
			}
		}
		return err
	}

	tracelog.InfoLogger.Printf("auth client ok")

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.Authentication{Type: pgproto3.AuthTypeOk},
		&pgproto3.ParameterStatus{Name: "integer_datetimes", Value: "on"},
		&pgproto3.ParameterStatus{Name: "server_version", Value: "lolkekcheburek"},
		&pgproto3.ReadyForQuery{},
	} {
		if err :=
			cl.Send(msg); err != nil {

			tracelog.InfoLogger.Printf("server starsup resp failed %v", msg)

			return err
		}
	}

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
	if db, ok := cl.sm.Parameters["database"]; ok {
		return db
	}

	return defaultUsr
}

func (cl *ShClient) receivepasswd() string {
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

func (cl *ShClient) PasswordCT() string {
	if db, ok := cl.sm.Parameters["password"]; ok {
		return db
	}

	_ = cl.be.Send(&pgproto3.Authentication{
		Type: pgproto3.AuthTypeCleartextPassword,
	})

	return cl.receivepasswd()
}

func (cl *ShClient) PasswordMD5() string {
	_ = cl.be.Send(&pgproto3.Authentication{
		Type: pgproto3.AuthTypeMD5Password,
		Salt: [4]byte{1, 3, 3, 7},
	})

	return cl.receivepasswd()
}

func (cl *ShClient) Receive() (pgproto3.FrontendMessage, error) {
	return cl.be.Receive()
}

func (cl *ShClient) Send(msg pgproto3.BackendMessage) error {
	return cl.be.Send(msg)
}

func (cl *ShClient) AssignRoute(r *Route) {
	cl.r = r
}

func (cl *ShClient) ProcQuery(query *pgproto3.Query) (byte, error) {

	if err := cl.shconn.Send(query); err != nil {
		return 0, err
	}

	for {
		msg, err := cl.shconn.Receive()
		if err != nil {
			return 0, err
		}
		switch v := msg.(type) {
		case *pgproto3.ReadyForQuery:
			return v.TxStatus, nil
		}

		err = cl.Send(msg)
		if err != nil {
			//tracelog.InfoLogger.Println(reflect.TypeOf(msg))
			//tracelog.InfoLogger.Println(msg)
			return 0, err
		}
	}
}

func (cl *ShClient) AssignShrdConn(srv *ShServer) {
	cl.shconn = srv
}

func (cl *ShClient) Route() *Route {
	return cl.r
}

func (cl *ShClient) DefaultReply() error {
	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.Authentication{Type: pgproto3.AuthTypeOk},
		&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
			{
				Name:                 "fortune",
				TableOID:             0,
				TableAttributeNumber: 0,
				DataTypeOID:          25,
				DataTypeSize:         -1,
				TypeModifier:         -1,
				Format:               0,
			},
		}},
		&pgproto3.DataRow{Values: [][]byte{[]byte("loh")}},
		&pgproto3.CommandComplete{CommandTag: "SELECT 1"},
		&pgproto3.ReadyForQuery{},
	} {
		if err := cl.Send(msg); err != nil {
			return err
		}
	}

	return nil
}

func (cl *ShClient) Rule() *FRRule {
	return cl.rule
}
