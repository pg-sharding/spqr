package client

import (
	"bufio"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"net"

	"github.com/jackc/pgproto3/v2"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"golang.org/x/xerrors"

	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/router/pkg/route"
	"github.com/pg-sharding/spqr/router/pkg/server"
)

var NotRouted = xerrors.New("client not routed")

type RouterClient interface {
	client.Client

	Server() server.Server
	Unroute() error

	AssignRule(rule *config.FRRule) error
	AssignServerConn(srv server.Server) error
	AssignRoute(r *route.Route) error

	Route() *route.Route
	Rule() *config.FRRule

	ProcQuery(query pgproto3.FrontendMessage, waitForResp bool) (byte, error)
	ProcCopy(query *pgproto3.FrontendMessage) error
	ProcCopyComplete(query *pgproto3.FrontendMessage) error
	ReplyParseComplete() error
}

type PsqlClient struct {
	client.Client
	params map[string]string
	rule   *config.FRRule
	conn   net.Conn

	r *route.Route

	id string

	be *pgproto3.Backend

	startupMsg *pgproto3.StartupMessage
	server     server.Server
}

func (cl *PsqlClient) SetParam(p *pgproto3.ParameterStatus) error {
	cl.params[p.Name] = p.Value
	return nil
}

func (cl *PsqlClient) Reply(msg string) error {
	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
			{
				Name:                 []byte("psql client"),
				TableOID:             0,
				TableAttributeNumber: 0,
				DataTypeOID:          25,
				DataTypeSize:         -1,
				TypeModifier:         -1,
				Format:               0,
			},
		}},
		&pgproto3.DataRow{Values: [][]byte{[]byte(msg)}},
		&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
		&pgproto3.ReadyForQuery{},
	} {
		if err := cl.Send(msg); err != nil {
			return err
		}
	}

	return nil
}

func (cl *PsqlClient) ReplyParseComplete() error {
	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.ParseComplete{},
		&pgproto3.ReadyForQuery{},
	} {
		if err := cl.Send(msg); err != nil {
			return err
		}
	}

	return nil
}

func (cl *PsqlClient) Reset() error {
	if cl.server == nil {
		return nil
	}
	return cl.server.Reset()
}

func (cl *PsqlClient) ReplyNotice(msg string) error {
	return nil
	if v, ok := cl.params["client_min_messages"]; ok {
		switch v {
		case "error":
			fallthrough
		case "warning":
			fallthrough
		case "fatal":
			return nil
		}
	}

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

func NewPsqlClient(pgconn net.Conn) *PsqlClient {
	cl := &PsqlClient{
		params:     make(map[string]string),
		conn:       pgconn,
		startupMsg: &pgproto3.StartupMessage{},
	}
	cl.id = "dwoiewiwe"

	return cl
}

func (cl *PsqlClient) Rule() *config.FRRule {
	return cl.rule
}

func (cl *PsqlClient) Server() server.Server {
	return cl.server
}

func (cl *PsqlClient) Unroute() error {
	if cl.server == nil {
		return NotRouted
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

	protoVer := binary.BigEndian.Uint32(msg)

	//tracelog.InfoLogger.Printf("prot version %s\n", protoVer)

	switch protoVer {
	case conn.SSLREQ:
		if sslmode == config.SSLMODEDISABLE {
			cl.be = pgproto3.NewBackend(pgproto3.NewChunkReader(bufio.NewReader(cl.conn)), cl.conn)
			tracelog.ErrorLogger.FatalOnError(err)
			return xerrors.Errorf("ssl mode is requested but ssl is disabled")
		}
		_, err := cl.conn.Write([]byte{'S'})
		if err != nil {
			return err
		}

		cl.conn = tls.Server(cl.conn, cfg)

		backend = pgproto3.NewBackend(pgproto3.NewChunkReader(bufio.NewReader(cl.conn)), cl.conn)

		frsm, err := backend.ReceiveStartupMessage()

		switch msg := frsm.(type) {
		case *pgproto3.StartupMessage:
			sm = msg
		default:
			return xerrors.Errorf("got unexpected message type %T", frsm)
		}

		if err != nil {
			return err
		}
	//
	case pgproto3.ProtocolVersionNumber:
		// reuse
		sm = &pgproto3.StartupMessage{}
		err = sm.Decode(msg)
		tracelog.ErrorLogger.FatalOnError(err)

		backend = pgproto3.NewBackend(pgproto3.NewChunkReader(bufio.NewReader(cl.conn)), cl.conn)
		tracelog.ErrorLogger.FatalOnError(err)

	case conn.CANCELREQ:
		fallthrough
	default:
		return xerrors.Errorf("protocol number %d not supported", protoVer)
	}

	cl.startupMsg = sm
	cl.be = backend

	if sslmode == config.SSLMODEREQUIRE && protoVer != conn.SSLREQ {
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
		&pgproto3.AuthenticationOk{},
		&pgproto3.ParameterStatus{Name: "integer_datetimes", Value: "on"},
		&pgproto3.ParameterStatus{Name: "server_version", Value: "spqr"},
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

	_ = cl.be.Send(&pgproto3.AuthenticationCleartextPassword{})

	return cl.receivepasswd()
}

func (cl *PsqlClient) PasswordMD5() string {
	_ = cl.be.Send(&pgproto3.AuthenticationMD5Password{
		Salt: [4]byte{1, 3, 3, 7},
	})

	return cl.receivepasswd()
}

func (cl *PsqlClient) Receive() (pgproto3.FrontendMessage, error) {
	return cl.be.Receive()
}

func (cl *PsqlClient) Send(msg pgproto3.BackendMessage) error {
	tracelog.InfoLogger.Printf("sending %T", msg)
	return cl.be.Send(msg)
}

func (cl *PsqlClient) AssignRoute(r *route.Route) error {
	if cl.r != nil {
		return xerrors.New("client already has assigned route")
	}

	cl.r = r
	return nil
}

func (cl *PsqlClient) ProcCopy(query *pgproto3.FrontendMessage) error {
	tracelog.InfoLogger.Printf("process copy %T", query)
	_ = cl.ReplyNotice(fmt.Sprintf("executing your query %v", query))
	return cl.server.Send(*query)
}

func (cl *PsqlClient) ProcCopyComplete(query *pgproto3.FrontendMessage) error {
	tracelog.InfoLogger.Printf("process copy end %T", query)
	if err := cl.server.Send(*query); err != nil {
		return err
	}

	for {
		if msg, err := cl.server.Receive(); err != nil {
			return err
		} else {
			switch msg.(type) {
			case *pgproto3.CommandComplete, *pgproto3.ErrorResponse:
				if err := cl.Send(msg); err != nil {
					return err
				}
				return nil
			default:
			}
		}
	}
}

func (cl *PsqlClient) ProcQuery(query pgproto3.FrontendMessage, waitForResp bool) (byte, error) {
	tracelog.InfoLogger.Printf("process query %s", query)
	_ = cl.ReplyNotice(fmt.Sprintf("executing your query %v", query))

	if err := cl.server.Send(query); err != nil {
		return 0, err
	}

	if !waitForResp {
		return conn.TXCOPY, nil
	}

	for {
		msg, err := cl.server.Receive()
		if err != nil {
			return 0, err
		}

		switch v := msg.(type) {
		case *pgproto3.CopyInResponse:
			err = cl.Send(msg)
			if err != nil {
				////tracelog.InfoLogger.Println(reflect.TypeOf(msg))
				return 0, err
			}

			if err := func() error {
				for {
					cpMsg, err := cl.Receive()
					if err != nil {
						return err
					}

					switch cpMsg.(type) {
					case *pgproto3.CopyData:
						if err := cl.ProcCopy(&cpMsg); err != nil {
							return err
						}
					case *pgproto3.CopyDone, *pgproto3.CopyFail:
						if err := cl.ProcCopyComplete(&cpMsg); err != nil {
							return err
						}
						return nil
					}
				}
			}(); err != nil {
				return 0, err
			}
		case *pgproto3.ReadyForQuery:
			return v.TxStatus, nil
		default:
			tracelog.InfoLogger.Printf("got msg type: %T", v)
			err = cl.Send(msg)
			if err != nil {
				////tracelog.InfoLogger.Println(reflect.TypeOf(msg))
				////tracelog.InfoLogger.Println(msg)
				return 0, err
			}
		}
	}
}

func (cl *PsqlClient) AssignServerConn(srv server.Server) error {
	if cl.server != nil {
		return xerrors.New("client already has active connection")
	}
	cl.server = srv

	return nil
}

func (cl *PsqlClient) Route() *route.Route {
	return cl.r
}

func (cl *PsqlClient) DefaultReply() error {
	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
			{
				Name:                 []byte("psql client"),
				TableOID:             0,
				TableAttributeNumber: 0,
				DataTypeOID:          25,
				DataTypeSize:         -1,
				TypeModifier:         -1,
				Format:               0,
			},
		}},
		&pgproto3.DataRow{Values: [][]byte{[]byte("no data")}},
		&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
		&pgproto3.ReadyForQuery{},
	} {
		if err := cl.Send(msg); err != nil {
			return err
		}
	}

	return nil
}

func (cl *PsqlClient) ReplyErrMsg(errmsg string) error {
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
			Message: "worldmock is shutdown, your connection closed",
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

var _ RouterClient = &PsqlClient{}

type FakeClient struct {
	RouterClient
}

func (f FakeClient) Usr() string {
	return DefaultUsr
}

func (f FakeClient) DB() string {
	return DefaultDB
}

func NewFakeClient() *FakeClient {
	return &FakeClient{}
}

func (f FakeClient) ID() string {
	return "fdijoidjs"
}

func (f FakeClient) Receive() (pgproto3.FrontendMessage, error) {
	return &pgproto3.Query{}, nil
}

func (f FakeClient) Send(msg pgproto3.BackendMessage) error {
	return nil
}

var _ RouterClient = &FakeClient{}
