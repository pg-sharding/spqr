package client

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"

	"github.com/jackc/pgproto3/v2"
	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/router/route"
	"github.com/pg-sharding/spqr/router/server"
)

var NotRouted = fmt.Errorf("client not routed")

type PreparedStatementMapper interface {
	PreparedStatementQueryByName(name string) string
	StorePreparedStatement(name, query string)
}

type RouterClient interface {
	client.Client
	PreparedStatementMapper

	Server() server.Server
	Unroute() error

	Auth(rt *route.Route) error

	AssignRule(rule *config.FrontendRule) error
	AssignServerConn(srv server.Server) error
	AssignRoute(r *route.Route) error

	Route() *route.Route
	Rule() *config.FrontendRule

	/* Client target-session-attrs policy */
	GetTsa() string
	SetTsa(string)

	ProcCommand(query pgproto3.FrontendMessage, waitForResp bool, replyCl bool) error
	ProcParse(query pgproto3.FrontendMessage, waitForResp bool, replyCl bool) error
	ProcQuery(query pgproto3.FrontendMessage, waitForResp bool, replyCl bool) (conn.TXStatus, bool, error)
	ProcCopy(query pgproto3.FrontendMessage) error
	ProcCopyComplete(query *pgproto3.FrontendMessage) error
	ReplyParseComplete() error

	CancelMsg() *pgproto3.CancelRequest

	GetCancelPid() uint32
	GetCancelKey() uint32

	Close() error
}

type PsqlClient struct {
	client.Client

	activeParamSet      map[string]string
	savepointParamSet   map[string]map[string]string
	savepointParamTxCnt map[string]int
	beginTxParamSet     map[string]string

	/* cancel */
	csm *pgproto3.CancelRequest

	cancel_pid uint32
	cancel_key uint32

	txCnt int

	rule *config.FrontendRule
	conn net.Conn

	r *route.Route

	id        string
	prepStmts map[string]string

	/* target-session-attrs */
	tsa string

	be *pgproto3.Backend

	startupMsg *pgproto3.StartupMessage
	server     server.Server
}

func (cl *PsqlClient) GetCancelPid() uint32 {
	return cl.cancel_pid
}

func (cl *PsqlClient) GetCancelKey() uint32 {
	return cl.cancel_key
}

func copymap(params map[string]string) map[string]string {
	ret := make(map[string]string)

	for k, v := range params {
		ret[k] = v
	}

	return ret
}

func (cl *PsqlClient) StartTx() {
	spqrlog.Logger.Printf(spqrlog.DEBUG4, "start new params set")
	cl.beginTxParamSet = copymap(cl.activeParamSet)
	cl.savepointParamSet = nil
	cl.savepointParamTxCnt = nil
	cl.txCnt = 0
}

func (cl *PsqlClient) CommitActiveSet() {
	cl.beginTxParamSet = nil
	cl.savepointParamSet = nil
	cl.savepointParamTxCnt = nil
	cl.txCnt = 0
}

func (cl *PsqlClient) Savepoint(name string) {
	cl.savepointParamSet[name] = copymap(cl.activeParamSet)
	cl.savepointParamTxCnt[name] = cl.txCnt
	cl.txCnt++
}

func (cl *PsqlClient) Rollback() {
	cl.activeParamSet = copymap(cl.beginTxParamSet)
	cl.beginTxParamSet = nil
	cl.savepointParamSet = nil
	cl.savepointParamTxCnt = nil
	cl.txCnt = 0
}

func (cl *PsqlClient) RollbackToSp(name string) {
	cl.activeParamSet = cl.savepointParamSet[name]
	targetTxCnt := cl.savepointParamTxCnt[name]
	for k := range cl.savepointParamSet {
		if cl.savepointParamTxCnt[k] > targetTxCnt {
			delete(cl.savepointParamTxCnt, k)
			delete(cl.savepointParamSet, k)
		}
	}
	cl.txCnt = targetTxCnt + 1
}

func (cl *PsqlClient) ConstructClientParams() *pgproto3.Query {
	query := &pgproto3.Query{
		String: "RESET ALL;",
	}

	for k, v := range cl.Params() {
		if k == "user" {
			continue
		}
		if k == "database" {
			continue
		}
		if k == "options" {
			continue
		}
		if k == "password" {
			continue
		}

		query.String += fmt.Sprintf("SET %s='%s';", k, v)
	}

	return query
}

func (cl *PsqlClient) ResetAll() {
	cl.activeParamSet = cl.startupMsg.Parameters
}

func (cl *PsqlClient) ProcParse(query pgproto3.FrontendMessage, waitForResp bool, replyCl bool) error {
	spqrlog.Logger.Printf(spqrlog.DEBUG1, "process parse %v", query)
	_ = cl.ReplyDebugNotice(fmt.Sprintf("executing your query %v", query))

	if err := cl.server.Send(query); err != nil {
		return err
	}

	if !waitForResp {
		return nil
	}

	for {
		msg, err := cl.server.Receive()
		if err != nil {
			return err
		}

		switch msg.(type) {
		case *pgproto3.ParseComplete:
			return nil
		default:
			if replyCl {
				err = cl.Send(msg)
				if err != nil {
					return err
				}
			}
		}
	}
}

func (cl *PsqlClient) StorePreparedStatement(name, query string) {
	cl.prepStmts[name] = query
}

func (cl *PsqlClient) PreparedStatementQueryByName(name string) string {
	if v, ok := cl.prepStmts[name]; ok {
		return v
	}
	return ""
}

func (cl *PsqlClient) ResetParam(name string) {
	if val, ok := cl.startupMsg.Parameters[name]; ok {
		cl.activeParamSet[name] = val
	} else {
		delete(cl.activeParamSet, name)
	}
	spqrlog.Logger.Printf(spqrlog.DEBUG2, "activeParamSet are now %+v", cl.activeParamSet)
}

func (cl *PsqlClient) SetParam(name, value string) {
	spqrlog.Logger.Printf(spqrlog.DEBUG1, "client param %v %v", name, value)
	if name == "options" {
		i := 0
		j := 0
		for i < len(value) {
			if value[i] == ' ' {
				i++
				continue
			}
			if value[i] == '-' {
				if i+2 == len(value) || value[i+1] != 'c' {
					// bad
					return
				}
			}
			i += 3
			j = i

			opname := ""
			opvalue := ""

			for j < len(value) {
				if value[j] == '=' {
					j++
					break
				}
				opname += string(value[j])
				j++
			}

			for j < len(value) {
				if value[j] == ' ' {
					break
				}
				opvalue += string(value[j])
				j++
			}

			if len(opname) == 0 || len(opvalue) == 0 {
				// bad
				return
			}
			i = j + 1

			spqrlog.Logger.Printf(spqrlog.DEBUG1, "parsed pgoption param %v %v", opname, opvalue)
			cl.activeParamSet[opname] = opvalue
		}

	} else {
		cl.activeParamSet[name] = value
	}
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

func (cl *PsqlClient) ReplyNotice(message string) error {
	return cl.Send(&pgproto3.NoticeResponse{
		Message: "NOTICE: " + message,
	})
}

func (cl *PsqlClient) ReplyDebugNotice(msg string) error {
	if v, ok := cl.activeParamSet["client_min_messages"]; !ok {
		return nil
	} else {
		if v != "notice" {
			return nil
		}
	}

	return cl.Send(&pgproto3.NoticeResponse{
		Message: "ROUTER NOTICE: " + msg,
	})
}

func (cl *PsqlClient) ReplyDebugNoticef(fmtString string, args ...interface{}) error {
	return cl.ReplyDebugNotice(fmt.Sprintf(fmtString, args...))
}

func (cl *PsqlClient) ID() string {
	return cl.id
}

func NewPsqlClient(pgconn net.Conn) *PsqlClient {
	cl := &PsqlClient{
		activeParamSet: make(map[string]string),
		conn:           pgconn,
		startupMsg:     &pgproto3.StartupMessage{},
		prepStmts:      map[string]string{},
		tsa:            config.TargetSessionAttrsRW,
	}
	cl.id = fmt.Sprintf("%p", cl)

	return cl
}

func (cl *PsqlClient) Rule() *config.FrontendRule {
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

func (cl *PsqlClient) AssignRule(rule *config.FrontendRule) error {
	if cl.rule != nil {
		return fmt.Errorf("client has active rule %s:%s", rule.Usr, rule.DB)
	}
	cl.rule = rule

	return nil
}

// startup + ssl/cancel
func (cl *PsqlClient) Init(tlsconfig *tls.Config) error {
	spqrlog.Logger.Printf(spqrlog.LOG, "init client connection with ssl: %t", tlsconfig != nil)

	for {
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

		spqrlog.Logger.Printf(spqrlog.DEBUG5, "got protoc version %d for client %p", protoVer, cl)

		if protoVer == conn.SSLREQ && tlsconfig == nil {
			_, err := cl.conn.Write([]byte{'N'})
			if err != nil {
				return err
			}
			// proceed next iter, for protocol version number or GSSAPI interaction
			continue
		}

		if protoVer == conn.GSSREQ {
			spqrlog.Logger.Printf(spqrlog.DEBUG1, "negotiate gss enc request")
			_, err := cl.conn.Write([]byte{'N'})
			if err != nil {
				return err
			}
			// proceed next iter, for protocol version number or GSSAPI interaction
			continue
		}

		switch protoVer {
		case conn.SSLREQ:
			_, err := cl.conn.Write([]byte{'S'})
			if err != nil {
				return err
			}

			cl.conn = tls.Server(cl.conn, tlsconfig)

			backend = pgproto3.NewBackend(pgproto3.NewChunkReader(bufio.NewReader(cl.conn)), cl.conn)

			frsm, err := backend.ReceiveStartupMessage()

			switch msg := frsm.(type) {
			case *pgproto3.StartupMessage:
				sm = msg
			default:
				return fmt.Errorf("got unexpected message type %T", frsm)
			}

			if err != nil {
				return err
			}
		//
		case pgproto3.ProtocolVersionNumber:
			// reuse
			sm = &pgproto3.StartupMessage{}
			err = sm.Decode(msg)
			if err != nil {
				spqrlog.Logger.PrintError(err)
				return err
			}
			backend = pgproto3.NewBackend(pgproto3.NewChunkReader(bufio.NewReader(cl.conn)), cl.conn)
			if err != nil {
				spqrlog.Logger.PrintError(err)
				return err
			}
		case conn.CANCELREQ:
			cl.csm = &pgproto3.CancelRequest{}
			if err = cl.csm.Decode(msg); err != nil {
				return err
			}

			return nil
		case conn.GSSREQ:
			/* TODO: Support */
		default:
			return fmt.Errorf("protocol number %d not supported", protoVer)
		}

		/* setup client params */

		for k, v := range sm.Parameters {
			cl.SetParam(k, v)
		}

		cl.startupMsg = sm
		cl.be = backend

		cl.cancel_key = rand.Uint32()
		cl.cancel_pid = rand.Uint32()

		spqrlog.Logger.Printf(spqrlog.DEBUG2, "client %p cancel key/pid: %d %d", cl, cl.cancel_key, cl.cancel_pid)

		if tlsconfig != nil && protoVer != conn.SSLREQ {
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
}

func (cl *PsqlClient) Auth(rt *route.Route) error {
	spqrlog.Logger.Printf(spqrlog.LOG, "processing frontend auth for %v %v\n", cl.Usr(), cl.DB())

	if err := conn.AuthFrontend(cl, cl.Rule()); err != nil {
		for _, msg := range []pgproto3.BackendMessage{
			&pgproto3.ErrorResponse{
				Message: fmt.Sprintf("auth failed %s", err),
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
	} {
		if err := cl.Send(msg); err != nil {
			return err
		}
	}

	spqrlog.Logger.Printf(spqrlog.LOG, "client connection for %v %v accepted\n", cl.Usr(), cl.DB())

	ps, err := rt.Params()
	if err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	for key, msg := range ps {
		if err := cl.Send(&pgproto3.ParameterStatus{
			Name:  key,
			Value: msg,
		}); err != nil {
			return err
		}
	}

	if err := cl.Send(&pgproto3.BackendKeyData{
		ProcessID: cl.cancel_pid,
		SecretKey: cl.cancel_key,
	}); err != nil {
		return err
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.ReadyForQuery{
			TxStatus: byte(conn.TXIDLE),
		},
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

	return DefaultDB
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

func (cl *PsqlClient) PasswordMD5(salt [4]byte) string {
	_ = cl.be.Send(&pgproto3.AuthenticationMD5Password{
		Salt: salt,
	})

	return cl.receivepasswd()
}

func (cl *PsqlClient) Receive() (pgproto3.FrontendMessage, error) {
	msg, err := cl.be.Receive()
	spqrlog.Logger.Printf(spqrlog.DEBUG5, "client %p: received message %+v", cl, msg)
	return msg, err
}

func (cl *PsqlClient) Send(msg pgproto3.BackendMessage) error {
	spqrlog.Logger.Printf(spqrlog.DEBUG5, "client %p: sending %+v", cl, msg)
	return cl.be.Send(msg)
}

func (cl *PsqlClient) SendCtx(ctx context.Context, msg pgproto3.BackendMessage) error {
	spqrlog.Logger.Printf(spqrlog.DEBUG3, "client %p: sending %T", cl, msg)
	ch := make(chan error)
	go func() {
		ch <- cl.be.Send(msg)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-ch:
		return err
	}
}

func (cl *PsqlClient) AssignRoute(r *route.Route) error {
	if cl.r != nil {
		return fmt.Errorf("client %p already has assigned route", cl)
	}

	cl.r = r
	return nil
}

func (cl *PsqlClient) ProcCopy(query pgproto3.FrontendMessage) error {
	spqrlog.Logger.Printf(spqrlog.DEBUG2, "client %p: process copy %T", cl, query)
	_ = cl.ReplyDebugNotice(fmt.Sprintf("executing your query %v", query))
	return cl.server.Send(query)
}

func (cl *PsqlClient) ProcCopyComplete(query *pgproto3.FrontendMessage) error {
	spqrlog.Logger.Printf(spqrlog.DEBUG2, "client %p: process copy end %T", cl, query)
	if err := cl.server.Send(*query); err != nil {
		return err
	}

	for {
		if msg, err := cl.server.Receive(); err != nil {
			return err
		} else {
			switch msg.(type) {
			case *pgproto3.CommandComplete, *pgproto3.ErrorResponse:
				return cl.Send(msg)
			default:
				if err := cl.Send(msg); err != nil {
					return err
				}
			}
		}
	}
}

func (cl *PsqlClient) ProcQuery(query pgproto3.FrontendMessage, waitForResp bool, replyCl bool) (conn.TXStatus, bool, error) {
	spqrlog.Logger.Printf(spqrlog.DEBUG2, "cleint %p process query %T", cl, query)
	_ = cl.ReplyDebugNoticef("executing your query %v", query)

	if cl.server == nil {
		return conn.TXERR, false, fmt.Errorf("client %p is out of transaction sync with router", cl)
	}
	if err := cl.server.Send(query); err != nil {
		return 0, false, err
	}

	if !waitForResp {
		return conn.TXCONT, true, nil
	}

	ok := true

	for {
		msg, err := cl.server.Receive()
		if err != nil {
			return 0, false, err
		}

		switch v := msg.(type) {
		case *pgproto3.CopyInResponse:
			// handle replyCl somehow
			err = cl.Send(msg)
			if err != nil {
				return 0, false, err
			}

			if err := func() error {
				for {
					cpMsg, err := cl.Receive()
					if err != nil {
						return err
					}

					switch cpMsg.(type) {
					case *pgproto3.CopyData:
						if err := cl.ProcCopy(cpMsg); err != nil {
							return err
						}
					case *pgproto3.CopyDone, *pgproto3.CopyFail:
						if err := cl.ProcCopyComplete(&cpMsg); err != nil {
							return err
						}
						return nil
					default:
					}
				}
			}(); err != nil {
				return conn.TXERR, false, err
			}
		case *pgproto3.ReadyForQuery:
			return conn.TXStatus(v.TxStatus), ok, nil
		case *pgproto3.ErrorResponse:
			if replyCl {
				err = cl.Send(msg)
				if err != nil {
					return conn.TXERR, false, err
				}
			}
			ok = false
		default:
			spqrlog.Logger.Printf(spqrlog.DEBUG2, "received msg type from server %p: %T", cl.server, v)
			if replyCl {
				err = cl.Send(msg)
				if err != nil {
					return conn.TXERR, false, err
				}
			}
		}
	}
}

func (cl *PsqlClient) ProcCommand(query pgproto3.FrontendMessage, waitForResp bool, replyCl bool) error {
	spqrlog.Logger.Printf(spqrlog.DEBUG2, "cleint %p process command %+v", cl, query)
	_ = cl.ReplyDebugNotice(fmt.Sprintf("executing your query %v", query))

	if err := cl.server.Send(query); err != nil {
		return err
	}

	if !waitForResp {
		return nil
	}

	for {
		msg, err := cl.server.Receive()
		if err != nil {
			return err
		}

		switch v := msg.(type) {
		case *pgproto3.CommandComplete:
			return nil
		case *pgproto3.ErrorResponse:
			return fmt.Errorf(v.Message)
		default:
			spqrlog.Logger.Printf(spqrlog.DEBUG2, "client %p msg type from server: %T", cl, v)
			if replyCl {
				err = cl.Send(msg)
				if err != nil {
					return err
				}
			}
		}
	}
}

func (cl *PsqlClient) AssignServerConn(srv server.Server) error {
	if cl.server != nil {
		return fmt.Errorf("client already has active connection")
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

func (cl *PsqlClient) Close() error {
	return cl.conn.Close()
}

func (cl *PsqlClient) Params() map[string]string {
	return cl.activeParamSet
}

func (cl *PsqlClient) ReplyErrMsg(errmsg string) error {
	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.ErrorResponse{
			Message:  errmsg,
			Severity: "ERROR",
		},
		&pgproto3.ReadyForQuery{
			TxStatus: byte(conn.TXIDLE),
		},
	} {
		if err := cl.Send(msg); err != nil {
			return err
		}
	}

	return nil
}

func (cl *PsqlClient) ReplyRFQ() error {
	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.ReadyForQuery{
			TxStatus: byte(conn.TXIDLE),
		},
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

func (cl *PsqlClient) GetTsa() string {
	return cl.tsa
}

func (cl *PsqlClient) SetTsa(s string) {
	cl.tsa = s
}

var _ RouterClient = &PsqlClient{}

func (cl *PsqlClient) CancelMsg() *pgproto3.CancelRequest {
	return cl.csm
}

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
