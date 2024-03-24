package client

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync"

	"github.com/pg-sharding/spqr/pkg/models/spqrerror"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/auth"
	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/conn"
	routerproto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/pg-sharding/spqr/router/port"
	"github.com/pg-sharding/spqr/router/route"
	"github.com/pg-sharding/spqr/router/routehint"
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

	RLock()
	RUnlock()

	/* only call this function while holding lock */
	Server() server.Server
	/* functions for operation with cleint's server */
	ServerAcquireUse()
	ServerReleaseUse()

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

	CancelMsg() *pgproto3.CancelRequest

	ReplyParseComplete() error
	ReplyBindComplete() error
	ReplyCommandComplete(commandTag string) error

	GetCancelPid() uint32
	GetCancelKey() uint32
}

type PsqlClient struct {
	// client.Client

	activeParamSet      map[string]string
	internalParamSet    map[string]string
	savepointParamSet   map[string]map[string]string
	savepointParamTxCnt map[string]int
	beginTxParamSet     map[string]string

	/* cancel */
	csm *pgproto3.CancelRequest

	cancel_pid uint32
	cancel_key uint32

	txCnt         int
	ReplyClientId bool

	rule *config.FrontendRule
	conn conn.RawConn

	r *route.Route

	prepStmts map[string]string

	/* target-session-attrs */
	tsa        string
	defaultTsa string

	/* protects client.Send() (backend) */
	muBe sync.Mutex
	be   *pgproto3.Backend

	startupMsg *pgproto3.StartupMessage

	bindParams [][]byte

	paramCodes []int16

	rh routehint.RouteHint

	/* protects server */
	mu     sync.RWMutex
	server server.Server
}

// BindParamFormatCodes implements RouterClient.
func (cl *PsqlClient) BindParamFormatCodes() []int16 {
	return cl.paramCodes
}

// SetParamFormatCodes implements RouterClient.
func (cl *PsqlClient) SetParamFormatCodes(paramCodes []int16) {
	cl.paramCodes = paramCodes
}

// BindParams implements RouterClient.
func (cl *PsqlClient) BindParams() [][]byte {
	return cl.bindParams
}

// SetBindParams implements RouterClient.
func (cl *PsqlClient) SetBindParams(p [][]byte) {
	cl.bindParams = p
}

// SetShardingKey implements RouterClient.
func (cl *PsqlClient) SetShardingKey(k string) {
	cl.internalParamSet[session.SPQR_SHARDING_KEY] = k
}

// ShardingKey implements RouterClient.
func (cl *PsqlClient) ShardingKey() string {
	val := cl.internalParamSet[session.SPQR_SHARDING_KEY]
	return val
}

// DefaultRouteBehaviour implements RouterClient.
func (cl *PsqlClient) DefaultRouteBehaviour() string {
	val := cl.internalParamSet[session.SPQR_DEFAULT_ROUTE_BEHAVIOUR]
	return val
}

// SetDefaultRouteBehaviour implements RouterClient.
func (cl *PsqlClient) SetDefaultRouteBehaviour(b string) {
	cl.internalParamSet[session.SPQR_DEFAULT_ROUTE_BEHAVIOUR] = b
}

// TODO : implement, unit tests
// ReceiveCtx implements RouterClient.
func (*PsqlClient) ReceiveCtx(ctx context.Context) (pgproto3.FrontendMessage, error) {
	panic("unimplemented")
}

// RouteHint implements RouterClient.
func (cl *PsqlClient) RouteHint() routehint.RouteHint {
	return cl.rh
}

// SetRouteHint implements RouterClient.
func (cl *PsqlClient) SetRouteHint(rh routehint.RouteHint) {
	cl.rh = rh
}

func NewPsqlClient(pgconn conn.RawConn, pt port.RouterPortType, defaultRouteBehaviour string) *PsqlClient {
	tsa := config.TargetSessionAttrsRW

	// enforce default port behaviour
	if pt == port.RORouterPortType {
		tsa = config.TargetSessionAttrsPS
	}

	cl := &PsqlClient{
		activeParamSet: make(map[string]string),
		internalParamSet: map[string]string{
			session.SPQR_DISTRIBUTION:            "default",
			session.SPQR_DEFAULT_ROUTE_BEHAVIOUR: defaultRouteBehaviour,
		},
		conn:       pgconn,
		startupMsg: &pgproto3.StartupMessage{},
		prepStmts:  map[string]string{},
		tsa:        tsa,
		defaultTsa: tsa,
		rh:         routehint.EmptyRouteHint{},
	}

	return cl
}

func (cl *PsqlClient) RLock() {
	cl.mu.RLock()
}

func (cl *PsqlClient) RUnlock() {
	cl.mu.RUnlock()
}

func (cl *PsqlClient) GetCancelPid() uint32 {
	return cl.cancel_pid
}

func (cl *PsqlClient) GetCancelKey() uint32 {
	return cl.cancel_key
}

func (cl *PsqlClient) SetAuthType(t uint32) error {
	return cl.be.SetAuthType(t)
}

func copymap(params map[string]string) map[string]string {
	ret := make(map[string]string)

	for k, v := range params {
		ret[k] = v
	}

	return ret
}

func (cl *PsqlClient) StartTx() {
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

func (cl *PsqlClient) RollbackToSP(name string) {
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
	spqrlog.Zero.Debug().
		Interface("activeParamSet", cl.activeParamSet).
		Msg("activeParamSet are now")
}

func (cl *PsqlClient) SetParam(name, value string) {
	spqrlog.Zero.Debug().
		Str("name", name).
		Str("value", value).
		Msg("client param")
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

			spqrlog.Zero.Debug().
				Str("opname", opname).
				Str("opvalue", opvalue).
				Msg("parsed pgoption param")
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

func (cl *PsqlClient) ReplyCommandComplete(commandTag string) error {
	return cl.Send(&pgproto3.CommandComplete{CommandTag: []byte(commandTag)})
}

func (cl *PsqlClient) ReplyParseComplete() error {
	return cl.Send(&pgproto3.ParseComplete{})
}

func (cl *PsqlClient) ReplyBindComplete() error {
	return cl.Send(&pgproto3.BindComplete{})
}

func (cl *PsqlClient) Reset() error {
	cl.mu.Lock()
	defer cl.mu.Unlock()

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

func (cl *PsqlClient) ReplyWarningMsg(errmsg string) error {
	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.ErrorResponse{
			Message:  fmt.Sprintf("client %p: error %v", cl, errmsg),
			Severity: "WARNING",
		},
	} {
		if err := cl.Send(msg); err != nil {
			return err
		}
	}

	return nil
}

func (cl *PsqlClient) ReplyWarningf(fmtString string, args ...interface{}) error {
	return cl.ReplyWarningMsg(fmt.Sprintf(fmtString, args...))
}

func (cl *PsqlClient) ID() uint {
	return spqrlog.GetPointer(cl)
}

func (cl *PsqlClient) Shards() []shard.Shard {
	cl.mu.RLock()
	defer cl.mu.RUnlock()
	if cl.server != nil {
		return cl.server.Datashards()
	}
	return nil
}

func (cl *PsqlClient) Rule() *config.FrontendRule {
	return cl.rule
}

/* this is for un-protected access for client's server variable, in cases when
concurrent Unroute() is impossible */

func (cl *PsqlClient) Server() server.Server {
	return cl.server
}

/* This method can be called concurrently with Cancel() */
func (cl *PsqlClient) Unroute() error {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	if cl.server == nil {
		/* TBD: raise error here sometimes? */
		return nil
	}
	cl.server = nil
	cl.ResetTsa()
	return nil
}

/* This method can be called concurrently with Unroute() */
func (cl *PsqlClient) Cancel() error {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	if cl.server == nil {
		/* TBD: raise error here sometimes? */
		return nil
	}
	/* server is locked,  */
	return cl.server.Cancel()
}

/* This method can be called concurrently with Unroute() */
func (cl *PsqlClient) ServerAcquireUse() {
	cl.mu.RLock()
}

func (cl *PsqlClient) ServerReleaseUse() {
	cl.mu.RUnlock()
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
	spqrlog.Zero.Info().
		Bool("ssl", tlsconfig != nil).
		Msg("init client connection")

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

		spqrlog.Zero.Debug().
			Uint("client", cl.ID()).
			Uint32("proto-version", protoVer).
			Msg("received protocol version")

		switch protoVer {
		case conn.GSSREQ:
			spqrlog.Zero.Debug().Msg("negotiate gss enc request")
			_, err := cl.conn.Write([]byte{'N'})
			if err != nil {
				return err
			}
			// proceed next iter, for protocol version number or GSSAPI interaction
			continue

		case conn.SSLREQ:
			if tlsconfig == nil {
				_, err := cl.conn.Write([]byte{'N'})
				if err != nil {
					return err
				}
				// proceed next iter, for protocol version number or GSSAPI interaction
				continue
			}

			_, err := cl.conn.Write([]byte{'S'})
			if err != nil {
				return err
			}

			cl.conn = tls.Server(cl.conn, tlsconfig)

			backend = pgproto3.NewBackend(bufio.NewReader(cl.conn), cl.conn)

			frsm, err := backend.ReceiveStartupMessage()

			switch msg := frsm.(type) {
			case *pgproto3.StartupMessage:
				sm = msg
			default:
				return fmt.Errorf("received unexpected message type %T", frsm)
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
				spqrlog.Zero.Error().Err(err).Msg("")
				return err
			}
			backend = pgproto3.NewBackend(bufio.NewReader(cl.conn), cl.conn)
		case conn.CANCELREQ:
			cl.csm = &pgproto3.CancelRequest{}
			if err = cl.csm.Decode(msg); err != nil {
				return err
			}

			return nil
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

		spqrlog.Zero.Debug().
			Uint("client", cl.ID()).
			Uint32("cancel_key", cl.cancel_key).
			Uint32("cancel_pid", cl.cancel_pid)

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
	spqrlog.Zero.Info().
		Str("user", cl.Usr()).
		Str("db", cl.DB()).
		Msg("processing frontend auth")

	if err := auth.AuthFrontend(cl, cl.Rule()); err != nil {
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

	spqrlog.Zero.Info().
		Uint("client", cl.ID()).
		Str("user", cl.Usr()).
		Str("db", cl.DB()).
		Msg("client connection for rule accepted")

	ps, err := rt.Params()
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
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
			TxStatus: byte(txstatus.TXIDLE),
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
const DefaultDS = "default"

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

func (cl *PsqlClient) receivepasswd() (string, error) {
	msg, err := cl.be.Receive()
	if err != nil {
		return "", err
	}

	switch v := msg.(type) {
	case *pgproto3.PasswordMessage:
		return v.Password, nil
	default:
		return "", fmt.Errorf("failed to receive password from backend msg")
	}
}

func (cl *PsqlClient) PasswordCT() (string, error) {
	if db, ok := cl.startupMsg.Parameters["password"]; ok {
		return db, nil
	}

	if err := cl.Send(&pgproto3.AuthenticationCleartextPassword{}); err != nil {
		return "", err
	}

	return cl.receivepasswd()
}

func (cl *PsqlClient) PasswordMD5(salt [4]byte) (string, error) {
	if err := cl.Send(&pgproto3.AuthenticationMD5Password{
		Salt: salt,
	}); err != nil {
		return "", err
	}
	return cl.receivepasswd()
}

func (cl *PsqlClient) Receive() (pgproto3.FrontendMessage, error) {
	msg, err := cl.be.Receive()
	spqrlog.Zero.Debug().
		Uint("client", cl.ID()).
		Interface("message", msg).
		Msg("client received message")
	return msg, err
}

func (cl *PsqlClient) Send(msg pgproto3.BackendMessage) error {
	spqrlog.Zero.Debug().
		Uint("client", cl.ID()).
		Type("msg-type", msg).
		Msg("sending msg to client")
	cl.muBe.Lock()
	defer cl.muBe.Unlock()
	cl.be.Send(msg)
	return cl.be.Flush()
}

func (cl *PsqlClient) SendCtx(ctx context.Context, msg pgproto3.BackendMessage) error {
	spqrlog.Zero.Debug().
		Uint("client", cl.ID()).
		Type("msg-type", msg).
		Msg("")
	ch := make(chan error)
	go func() {
		ch <- cl.Send(msg)
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

func (cl *PsqlClient) AssignServerConn(srv server.Server) error {
	cl.mu.Lock()
	defer cl.mu.Unlock()
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

func (cl *PsqlClient) ReplyErrMsg(msg string, code string) error {
	var clerrmsg string

	if cl.ReplyClientId {
		clerrmsg = fmt.Sprintf("client %p: error %v", cl, msg)
	} else {
		clerrmsg = msg
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.ErrorResponse{
			Message:  clerrmsg,
			Severity: "ERROR",
			Code:     code,
		},
		&pgproto3.ReadyForQuery{
			TxStatus: byte(txstatus.TXIDLE),
		},
	} {
		if err := cl.Send(msg); err != nil {
			return err
		}
	}
	return nil
}
func (cl *PsqlClient) ReplyErr(e error) error {
	switch er := e.(type) {
	case *spqrerror.SpqrError:
		return cl.ReplyErrMsg(er.Error(), er.ErrorCode)
	default:
		return cl.ReplyErrMsg(e.Error(), "SPQRU")
	}
}

func (cl *PsqlClient) ReplyErrMsgByCode(code string) error {
	clerrmsg := spqrerror.GetMessageByCode(code)
	return cl.ReplyErrMsg(clerrmsg, code)
}

func (cl *PsqlClient) ReplyRFQ(txstatus txstatus.TXStatus) error {
	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.ReadyForQuery{
			TxStatus: byte(txstatus),
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
			Message: "backend is shutdown, your connection closed",
		},
		&pgproto3.ReadyForQuery{},
	} {
		if err := cl.Send(msg); err != nil {
			return err
		}
	}

	_ = cl.Unroute()

	return cl.conn.Close()
}

func (cl *PsqlClient) GetTsa() string {
	return cl.tsa
}

func (cl *PsqlClient) SetTsa(s string) {
	cl.tsa = s
}

func (cl *PsqlClient) ResetTsa() {
	cl.tsa = cl.defaultTsa
}

func (cl *PsqlClient) CancelMsg() *pgproto3.CancelRequest {
	return cl.csm
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

func (f FakeClient) ID() uint {
	return spqrlog.GetPointer(f)
}

func (f FakeClient) Receive() (pgproto3.FrontendMessage, error) {
	return &pgproto3.Query{}, nil
}

func (f FakeClient) Send(msg pgproto3.BackendMessage) error {
	return nil
}

var _ RouterClient = &FakeClient{}

func NewNoopClient(clientInfo *routerproto.ClientInfo, rAddr string) NoopClient {
	client := NoopClient{
		id:     uint(clientInfo.ClientId),
		user:   clientInfo.User,
		dbname: clientInfo.Dbname,
		dsname: clientInfo.Dsname,
		rAddr:  rAddr,
		shards: make([]shard.Shard, len(clientInfo.Shards)),
	}
	for _, shardInfo := range clientInfo.Shards {
		client.shards = append(client.shards, MockShard{instance: MockDBInstance{hostname: shardInfo.Instance.Hostname}})
	}
	return client
}

type NoopClient struct {
	client.Client
	id     uint
	user   string
	dbname string
	dsname string
	shards []shard.Shard
	rAddr  string
}

func (c NoopClient) ID() uint {
	return c.id
}

func (c NoopClient) Usr() string {
	return c.user
}

func (c NoopClient) DB() string {
	return c.dbname
}

func (c NoopClient) RAddr() string {
	return c.rAddr
}

func (c NoopClient) Shards() []shard.Shard {
	return c.shards
}

type MockShard struct {
	shard.Shard

	instance MockDBInstance
}

func (s MockShard) Instance() conn.DBInstance {
	return s.instance
}

type MockDBInstance struct {
	conn.DBInstance

	hostname string
}

func (dbi MockDBInstance) Hostname() string {
	return dbi.hostname
}

var _ client.ClientInfo = &NoopClient{}
var _ shard.Shard = &MockShard{}
var _ conn.DBInstance = &MockDBInstance{}
