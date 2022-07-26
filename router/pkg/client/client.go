package client

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"net"

	"github.com/pg-sharding/spqr/pkg/spqrlog"

	"github.com/jackc/pgproto3/v2"
	"github.com/pkg/errors"
	"golang.org/x/xerrors"

	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/router/pkg/route"
	"github.com/pg-sharding/spqr/router/pkg/server"
)

var NotRouted = xerrors.New("client not routed")

type RouterPreparedStatement struct {
	query string
}

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

	ProcCommand(query pgproto3.FrontendMessage, waitForResp bool, replyCl bool) error
	ProcParse(query pgproto3.FrontendMessage, waitForResp bool, replyCl bool) error
	ProcQuery(query pgproto3.FrontendMessage, waitForResp bool, replyCl bool) (conn.TXStatus, bool, error)
	ProcCopy(query pgproto3.FrontendMessage) error
	ProcCopyComplete(query *pgproto3.FrontendMessage) error
	ReplyParseComplete() error
	Close() error
}

type PsqlClient struct {
	client.Client
	activeParamSet      map[string]string
	savepointParamSet   map[string]map[string]string
	savepointParamTxCnt map[string]int
	beginTxParamSet     map[string]string

	txCnt int

	rule *config.FrontendRule
	conn net.Conn

	r *route.Route

	id        string
	prepStmts map[string]string

	be *pgproto3.Backend

	startupMsg *pgproto3.StartupMessage
	server     server.Server
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

		query.String += fmt.Sprintf("SET %s='%s';", k, v)
	}

	return query
}

func (cl *PsqlClient) ResetAll() {
	cl.activeParamSet = cl.startupMsg.Parameters
}

func (cl *PsqlClient) ProcParse(query pgproto3.FrontendMessage, waitForResp bool, replyCl bool) error {
	spqrlog.Logger.Printf(spqrlog.DEBUG1, "process parse %v", query)
	_ = cl.ReplyNotice(fmt.Sprintf("executing your query %v", query))

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

func (cl *PsqlClient) ReplyShardMatch(shardId string) error {
	if v, ok := cl.activeParamSet["spqr_reply_shard_match"]; !ok {
		return nil
	} else {
		if v != "on" {
			return nil
		}
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.NoticeResponse{
			Message: "Shard match: " + shardId,
		},
	} {
		if err := cl.Send(msg); err != nil {
			return err
		}
	}

	return nil
}

func (cl *PsqlClient) ReplyNotice(msg string) error {
	if v, ok := cl.activeParamSet["client_min_messages"]; !ok {
		return nil
	} else {
		if v != "notice" {
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

func (cl *PsqlClient) ReplyNoticef(fmtString string, args ...interface{}) error {
	return cl.ReplyNotice(fmt.Sprintf(fmtString, args...))
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
	}
	cl.id = "dwoiewiwe"

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
		return xerrors.Errorf("client has active rule %s:%s", rule.User, rule.DB)
	}
	cl.rule = rule

	return nil
}

// startup + ssl
func (cl *PsqlClient) Init(tlsconfig *tls.Config) error {
	spqrlog.Logger.Printf(spqrlog.LOG, "initialing client connection with ssl: %t", tlsconfig == nil)

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

	switch protoVer {
	case conn.SSLREQ:
		if tlsconfig == nil {
			cl.be = pgproto3.NewBackend(pgproto3.NewChunkReader(bufio.NewReader(cl.conn)), cl.conn)
			spqrlog.Logger.Errorf("ssl mode is requested but ssl is disabled")
			_ = cl.ReplyErrMsg("ssl mode is requested but ssl is disabled")
			return fmt.Errorf("ssl mode is requested but ssl is disabled")
		}

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
		return fmt.Errorf("cancel is not supported")
	default:
		return fmt.Errorf("protocol number %d not supported", protoVer)
	}

	for k, v := range sm.Parameters {
		cl.SetParam(k, v)
	}

	cl.startupMsg = sm
	cl.be = backend

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

func (cl *PsqlClient) Auth(rt *route.Route) error {
	spqrlog.Logger.Printf(spqrlog.LOG, "Processing auth for %v %v\n", cl.User(), cl.DB())

	if err := func() error {
		switch cl.Rule().AuthRule.Method {
		case config.AuthOK:
			return nil
			// TODO:
		case config.AuthNotOK:
			return errors.Errorf("user %v %v blocked", cl.User(), cl.DB())
		case config.AuthClearText:
			if cl.PasswordCT() != cl.Rule().AuthRule.Password {
				return errors.Errorf("user %v %v auth failed", cl.User(), cl.DB())
			}
			return nil
		case config.AuthMD5:
			fallthrough
		case config.AuthSCRAM:
			fallthrough
		default:
			return errors.Errorf("invalid auth method %v", cl.Rule().AuthRule.Method)
		}
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
	} {
		if err := cl.Send(msg); err != nil {
			return err
		}
	}

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

func (cl *PsqlClient) User() string {
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

func (cl *PsqlClient) PasswordMD5() string {
	_ = cl.be.Send(&pgproto3.AuthenticationMD5Password{
		Salt: [4]byte{1, 3, 3, 7},
	})

	return cl.receivepasswd()
}

func (cl *PsqlClient) Receive() (pgproto3.FrontendMessage, error) {
	msg, err := cl.be.Receive()
	spqrlog.Logger.Printf(spqrlog.DEBUG3, "Received %T from client", msg)
	return msg, err
}

func (cl *PsqlClient) Send(msg pgproto3.BackendMessage) error {
	spqrlog.Logger.Printf(spqrlog.DEBUG3, "sending %T to client", msg)
	return cl.be.Send(msg)
}

func (cl *PsqlClient) SendCtx(ctx context.Context, msg pgproto3.BackendMessage) error {
	spqrlog.Logger.Printf(spqrlog.DEBUG3, "sending %T to client", msg)
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
		return xerrors.New("client already has assigned route")
	}

	cl.r = r
	return nil
}

func (cl *PsqlClient) ProcCopy(query pgproto3.FrontendMessage) error {
	spqrlog.Logger.Printf(spqrlog.DEBUG2, "process copy %T", query)
	_ = cl.ReplyNotice(fmt.Sprintf("executing your query %v", query))
	return cl.server.Send(query)
}

func (cl *PsqlClient) ProcCopyComplete(query *pgproto3.FrontendMessage) error {
	spqrlog.Logger.Printf(spqrlog.DEBUG2, "process copy end %T", query)
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
	spqrlog.Logger.Printf(spqrlog.DEBUG2, "process query %s", query)
	_ = cl.ReplyNoticef("executing your query %v", query)

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
			spqrlog.Logger.Printf(spqrlog.DEBUG2, "got msg type: %T", v)
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
	spqrlog.Logger.Printf(spqrlog.DEBUG2, "process command %+v", query)
	_ = cl.ReplyNotice(fmt.Sprintf("executing your query %v", query))

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
			return xerrors.New(v.Message)
		default:
			spqrlog.Logger.Printf(spqrlog.DEBUG2, "got msg type: %T", v)
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

func (f FakeClient) User() string {
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
