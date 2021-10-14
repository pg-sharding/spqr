package rrouter

import (
	"bufio"
	"crypto/tls"
	"encoding/binary"
	"net"

	"github.com/jackc/pgproto3"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/router/pkg/conn"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"golang.org/x/xerrors"
)

type Client interface {
	Server() Server
	Unroute() error

	ID() string

	AssignRule(rule *config.FRRule) error
	AssignServerConn(srv Server) error
	AssignRoute(r *Route) error

	ReplyErr(errmsg string) error
	ReplyNotice(msg string) error

	Init(cfg *tls.Config, reqssl string) error
	Auth() error
	StartupMessage() *pgproto3.StartupMessage

	Usr() string
	DB() string

	PasswordCT() string
	PasswordMD5() string
	DefaultReply() error

	Route() *Route
	Rule() *config.FRRule

	ProcQuery(query *pgproto3.Query) (byte, error)

	Send(msg pgproto3.BackendMessage) error
	Receive() (pgproto3.FrontendMessage, error)

	Shutdown() error
}

type PsqlClient struct {
	rule *config.FRRule
	conn net.Conn

	r *Route

	id string

	be *pgproto3.Backend

	startupMsg *pgproto3.StartupMessage
	server     Server
}

func (cl *PsqlClient) ReplyNotice(msg string) error {
	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.NoticeResponse{
			Message: "ROUTER NOTICE: " + msg,
		},
	} {
		if err := cl.Send(msg); err != nil {
			return err
		}
	}

	return nil
}

func (cl *PsqlClient) ID() string {
	return cl.id
}

func NewClient(pgconn net.Conn) Client {
	cl := &PsqlClient{
		conn:       pgconn,
		startupMsg: &pgproto3.StartupMessage{},
	}
	cl.id = "dwoiewiwe"

	return cl
}

func (cl *PsqlClient) Rule() *config.FRRule {
	return cl.rule
}

func (cl *PsqlClient) Server() Server {
	return cl.server
}

func (cl *PsqlClient) Unroute() error {
	if cl.server == nil {
		return xerrors.New("client not routed")
	}
	cl.server = nil

	return nil
}

func (cl *PsqlClient) AssignRule(rule *config.FRRule) error {
	if cl.rule != nil {
		return xerrors.Errorf("client has active rule %s:%s", rule.RK.Usr, rule.RK.DB)
	}
	cl.rule = rule

	return nil
}

// startup + ssl
func (cl *PsqlClient) Init(cfg *tls.Config, sslmode string) error {

	tracelog.InfoLogger.Printf("initialing client connection with %v ssl req", sslmode)

	var backend *pgproto3.Backend

	var sm *pgproto3.StartupMessage

	headerRaw := make([]byte, 4)

	_, err := cl.conn.Read(headerRaw)
	if err != nil {
		return err
	}
	msgSize := int(binary.BigEndian.Uint32(headerRaw) - 4)

	msg := make([]byte, msgSize)

	_, err = cl.conn.Read(msg)
	if err != nil {
		return err
	}

	protVer := binary.BigEndian.Uint32(msg)

	switch protVer {
	case conn.SSLREQ:
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
	//
	case pgproto3.ProtocolVersionNumber:
		// reuse
		sm = &pgproto3.StartupMessage{}
		err = sm.Decode(msg)
		tracelog.ErrorLogger.FatalOnError(err)

		backend, err = pgproto3.NewBackend(pgproto3.NewChunkReader(bufio.NewReader(cl.conn)), cl.conn)
		tracelog.ErrorLogger.FatalOnError(err)

	case conn.CANCELREQ:
		fallthrough
	default:
		return xerrors.Errorf("protocol number %d not supported", protVer)
	}

	cl.startupMsg = sm
	cl.be = backend

	if sslmode == config.SSLMODEREQUIRE && protVer != conn.SSLREQ {
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

func (cl *PsqlClient) Auth() error {
	tracelog.InfoLogger.Printf("Processing auth for %v %v\n", cl.Usr(), cl.DB())

	if err := func() error {
		switch cl.Rule().AuthRule.Method {
		case config.AuthOK:
			return nil
			// TODO:
		case config.AuthNotOK:
			return errors.Errorf("user %v %v blocked", cl.Usr(), cl.DB())
		case config.AuthClearText:
			if cl.PasswordCT() != cl.Rule().AuthRule.Password {
				return errors.Errorf("user %v %v auth failed", cl.Usr(), cl.DB())
			}

			return nil
		case config.AuthMD5:

		case config.AuthSCRAM:

		default:
			return errors.Errorf("invalid auth method %v", cl.Rule().AuthRule.Method)
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

func (cl *PsqlClient) StartupMessage() *pgproto3.StartupMessage {
	return cl.startupMsg
}

const DefaultUsr = "default"
const DefaultDB = "default"

func (cl *PsqlClient) Usr() string {
	if usr, ok := cl.startupMsg.Parameters["user"]; ok {
		return usr
	}

	return DefaultUsr
}

func (cl *PsqlClient) DB() string {
	if db, ok := cl.startupMsg.Parameters["database"]; ok {
		return db
	}

	return DefaultUsr
}

func (cl *PsqlClient) receivepasswd() string {
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

func (cl *PsqlClient) PasswordCT() string {
	if db, ok := cl.startupMsg.Parameters["password"]; ok {
		return db
	}

	_ = cl.be.Send(&pgproto3.Authentication{
		Type: pgproto3.AuthTypeCleartextPassword,
	})

	return cl.receivepasswd()
}

func (cl *PsqlClient) PasswordMD5() string {
	_ = cl.be.Send(&pgproto3.Authentication{
		Type: pgproto3.AuthTypeMD5Password,
		Salt: [4]byte{1, 3, 3, 7},
	})

	return cl.receivepasswd()
}

func (cl *PsqlClient) Receive() (pgproto3.FrontendMessage, error) {
	return cl.be.Receive()
}

func (cl *PsqlClient) Send(msg pgproto3.BackendMessage) error {
	return cl.be.Send(msg)
}

func (cl *PsqlClient) AssignRoute(r *Route) error {
	if cl.r != nil {
		return xerrors.New("client already has assigned route")
	}

	cl.r = r
	return nil
}

func (cl *PsqlClient) ProcQuery(query *pgproto3.Query) (byte, error) {

	tracelog.InfoLogger.Printf("process query %s", query)

	if err := cl.server.Send(query); err != nil {
		return 0, err
	}

	for {
		msg, err := cl.server.Receive()
		tracelog.InfoLogger.Printf("recv msg from server %v %w", msg, err)

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

func (cl *PsqlClient) AssignServerConn(srv Server) error {
	if cl.server != nil {
		return xerrors.New("client already has active connection")
	}
	cl.server = srv

	return nil
}

func (cl *PsqlClient) Route() *Route {
	return cl.r
}

func (cl *PsqlClient) DefaultReply() error {
	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
			{
				Name:                 "pkg",
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

func (cl *PsqlClient) ReplyErr(errmsg string) error {
	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.ErrorResponse{
			Message: errmsg,
		},
		&pgproto3.ReadyForQuery{},
	} {
		if err := cl.Send(msg); err != nil {
			return err
		}
	}

	return nil
}

func (cl *PsqlClient) Shutdown() error {
	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.ErrorResponse{
			Message: "pkg is shutdown, your connection closed",
		},
		&pgproto3.ReadyForQuery{},
	} {
		if err := cl.Send(msg); err != nil {
			return err
		}
	}

	_ = cl.Unroute()

	_ = cl.conn.Close()
	return nil
}

var _ Client = &PsqlClient{}

type FakeClient struct {
}

func (f FakeClient) ReplyNotice(msg string) error {
	panic("implement me")
}

func (f FakeClient) Shutdown() error {
	panic("implement me")
}

func NewFakeClient() *FakeClient {
	return &FakeClient{}
}

func (f FakeClient) Server() Server {
	return nil
}

func (f FakeClient) Unroute() error {
	return nil
}

func (f FakeClient) AssignRule(rule *config.FRRule) error {
	return nil
}

func (f FakeClient) AssignRoute(r *Route) error {
	return nil
}

func (f FakeClient) AssignServerConn(srv Server) error {
	return nil
}

func (f FakeClient) ReplyErr(errnsg string) error {
	return nil
}

func (f FakeClient) Init(cfg *tls.Config, reqssl string) error {
	return nil
}

func (f FakeClient) Auth() error {
	return nil
}

func (f FakeClient) StartupMessage() *pgproto3.StartupMessage {
	return nil
}

func (f FakeClient) Usr() string {
	return DefaultUsr
}

func (f FakeClient) DB() string {
	return DefaultDB
}

func (f FakeClient) PasswordCT() string {
	return ""
}

func (f FakeClient) PasswordMD5() string {
	return ""
}

func (f FakeClient) DefaultReply() error {
	return nil
}

func (f FakeClient) Route() *Route {
	return nil
}

func (f FakeClient) Rule() *config.FRRule {
	return nil
}

func (f FakeClient) ProcQuery(query *pgproto3.Query) (byte, error) {
	return 0, nil
}

func (f FakeClient) Send(msg pgproto3.BackendMessage) error {
	return nil
}

func (f FakeClient) ID() string {
	return "fdijoidjs"
}

func (f FakeClient) Receive() (pgproto3.FrontendMessage, error) {
	return &pgproto3.Query{}, nil
}

var _ Client = &FakeClient{}
