package client

import (
	"bufio"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"sync/atomic"
	"time"

	"github.com/caio/go-tdigest"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/prepstatement"
	"github.com/spaolacci/murmur3"

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
	"github.com/pg-sharding/spqr/router/server"
	"github.com/pg-sharding/spqr/router/statistics"
	"github.com/pg-sharding/spqr/router/twopc"
)

type RouterClient interface {
	client.Client
	prepstatement.PreparedStatementMapper

	session.SessionParamsHolder
	Server() server.Server
	/* functions for operation with client's server */

	Unroute() error

	Auth(rt *route.Route) error

	AssignRule(rule *config.FrontendRule) error
	AssignServerConn(srv server.Server) error
	SwitchServerConn(srv server.Server) error

	AssignRoute(r *route.Route) error

	Route() *route.Route
	Rule() *config.FrontendRule

	/* Client target-session-attrs policy */

	ResetTsa()

	ReplyParseComplete() error
	ReplyBindComplete() error
	ReplyCommandComplete(commandTag string) error

	GetCancelPid() uint32
	GetCancelKey() uint32
}

type PsqlClient struct {
	session.SessionParamsHolder
	/* cancel */
	csm *pgproto3.CancelRequest

	cancel_pid uint32
	cancel_key uint32

	ReplyClientId bool

	rule *config.FrontendRule
	conn conn.RawConn

	r *route.Route

	prepStmts     map[string]*prepstatement.PreparedStatementDefinition
	prepStmtsHash map[string]uint64

	be *pgproto3.Backend

	startupMsg *pgproto3.StartupMessage
	id         uint

	cacheCC pgproto3.CommandComplete

	RouterTime *tdigest.TDigest
	ShardTime  *tdigest.TDigest

	TimeData *statistics.StartTimes

	serverP atomic.Pointer[server.Server]
}

var _ RouterClient = &PsqlClient{}

// Add implements statistics.StatHolder.
func (r *PsqlClient) Add(st statistics.StatisticsType, value float64) error {
	switch st {
	case statistics.StatisticsTypeRouter:
		return r.RouterTime.Add(value)
	case statistics.StatisticsTypeShard:
		return r.ShardTime.Add(value)
	default:
		// panic?
		return nil
	}
}

// Conn implements RouterClient.
func (r *PsqlClient) Conn() net.Conn {
	return r.conn
}

// GetTimeData implements statistics.StatHolder.
func (r *PsqlClient) GetTimeData() *statistics.StartTimes {
	return r.TimeData
}

// GetTimeQuantile implements statistics.StatHolder.
func (r *PsqlClient) GetTimeQuantile(statType statistics.StatisticsType, q float64) float64 {

	switch statType {
	case statistics.StatisticsTypeRouter:
		if r.RouterTime.Count() == 0 {
			return 0
		}

		return r.RouterTime.Quantile(q)
	case statistics.StatisticsTypeShard:
		if r.ShardTime.Count() == 0 {
			return 0
		}

		return r.ShardTime.Quantile(q)
	default:
		return 0
	}
}

// RecordStartTime implements statistics.StatHolder.
func (r *PsqlClient) RecordStartTime(statType statistics.StatisticsType, t time.Time) {
	if r.TimeData == nil {
		r.TimeData = &statistics.StartTimes{}
	}

	switch statType {
	case statistics.StatisticsTypeRouter:
		r.TimeData.RouterStart = t
	case statistics.StatisticsTypeShard:
		r.TimeData.ShardStart = t
	}
}

func NewPsqlClient(pgconn conn.RawConn, pt port.RouterPortType, defaultRouteBehaviour string, showNoticeMessages bool, instanceDefaultTsa string) RouterClient {
	var target_session_attrs string
	if instanceDefaultTsa != "" {
		target_session_attrs = instanceDefaultTsa
	} else {
		target_session_attrs = config.TargetSessionAttrsRW
	}

	// enforce default port behaviour
	if pt == port.RORouterPortType {
		target_session_attrs = config.TargetSessionAttrsPS
	}

	sh := session.NewSimpleHandler(target_session_attrs, showNoticeMessages, twopc.COMMIT_STRATEGY_1PC, defaultRouteBehaviour)

	cl := &PsqlClient{
		SessionParamsHolder: sh,
		conn:                pgconn,
		startupMsg:          &pgproto3.StartupMessage{},
		prepStmts:           map[string]*prepstatement.PreparedStatementDefinition{},
		prepStmtsHash:       map[string]uint64{},

		serverP: atomic.Pointer[server.Server]{},
	}

	cl.SetCommitStrategy(twopc.COMMIT_STRATEGY_BEST_EFFORT)

	cl.id = spqrlog.GetPointer(cl)

	cl.serverP.Store(nil)

	cl.RouterTime, _ = tdigest.New()
	cl.ShardTime, _ = tdigest.New()

	return cl
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

		if len(k) >= 6 && k[0:6] == "__spqr" {
			continue
		}

		query.String += fmt.Sprintf("SET %s='%s';", k, v)
	}

	return query
}

func (cl *PsqlClient) StorePreparedStatement(d *prepstatement.PreparedStatementDefinition) {
	hash := murmur3.Sum64([]byte(d.Query))
	cl.prepStmts[d.Name] = d
	cl.prepStmtsHash[d.Name] = hash
}

func (cl *PsqlClient) PreparedStatementQueryByName(name string) string {
	if v, ok := cl.prepStmts[name]; ok {
		return v.Query
	}
	return ""
}

func (cl *PsqlClient) PreparedStatementDefinitionByName(name string) *prepstatement.PreparedStatementDefinition {
	if v, ok := cl.prepStmts[name]; ok {
		return v
	}
	return nil
}

func (cl *PsqlClient) PreparedStatementQueryHashByName(name string) uint64 {
	return cl.prepStmtsHash[name]
}

func (cl *PsqlClient) Reply(msg string) error {
	cl.cacheCC.CommandTag = []byte("SELECT 1")
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
		&cl.cacheCC,
		&pgproto3.ReadyForQuery{},
	} {
		if err := cl.Send(msg); err != nil {
			return err
		}
	}

	return nil
}

func (cl *PsqlClient) ReplyCommandComplete(commandTag string) error {
	cl.cacheCC.CommandTag = []byte(commandTag)
	return cl.Send(&cl.cacheCC)
}

var (
	bindCMsg  = &pgproto3.BindComplete{}
	parseCMsg = &pgproto3.ParseComplete{}
)

func (cl *PsqlClient) ReplyParseComplete() error {
	return cl.Send(parseCMsg)
}

func (cl *PsqlClient) ReplyBindComplete() error {
	return cl.Send(bindCMsg)
}

func (cl *PsqlClient) Reset() error {
	serv := cl.serverP.Load()

	if serv == nil || *serv == nil {
		return nil
	}
	return (*serv).Reset()
}

func (cl *PsqlClient) ReplyNotice(message string) error {
	return cl.Send(&pgproto3.NoticeResponse{
		Message: "NOTICE: " + message,
	})
}

func (cl *PsqlClient) ReplyDebugNotice(msg string) error {
	if v, ok := cl.Params()["client_min_messages"]; !ok {
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

func (cl *PsqlClient) ReplyDebugNoticef(fmtString string, args ...any) error {
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

func (cl *PsqlClient) ReplyWarningf(fmtString string, args ...any) error {
	return cl.ReplyWarningMsg(fmt.Sprintf(fmtString, args...))
}

func (cl *PsqlClient) ID() uint {
	return cl.id
}

func (cl *PsqlClient) Shards() []shard.ShardHostInstance {

	serv := cl.serverP.Load()

	if serv == nil || *serv == nil {
		return nil
	}

	return (*serv).Datashards()
}

func (cl *PsqlClient) Rule() *config.FrontendRule {
	return cl.rule
}

/* this is for un-protected access for client's server variable, in cases when
concurrent Unroute() is impossible */

func (cl *PsqlClient) Server() server.Server {
	serv := cl.serverP.Load()
	if serv == nil {
		return nil
	}
	return *serv
}

/* This method can be called concurrently with Cancel() */
func (cl *PsqlClient) Unroute() error {
	serv := cl.serverP.Load()

	if serv == nil || *serv == nil {
		/* TBD: raise error here sometimes? */
		return nil
	}

	cl.serverP.Store(nil)
	return nil
}

/* This method can be called concurrently with Unroute() */
func (cl *PsqlClient) Cancel() error {
	serv := cl.serverP.Load()

	if serv == nil || *serv == nil {
		/* TBD: raise error here sometimes? */
		return nil
	}

	/* server is locked,  */
	return (*serv).Cancel()
}

func (cl *PsqlClient) AssignRule(rule *config.FrontendRule) error {
	if cl.rule != nil {
		return fmt.Errorf("client has active rule %s:%s", rule.Usr, rule.DB)
	}
	cl.rule = rule

	return nil
}

const pingRoute = "spqr-ping"

// startup + ssl/cancel
func (cl *PsqlClient) Init(tlsconfig *tls.Config) error {
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

		spqrlog.Zero.Info().
			Uint("client", cl.ID()).
			Uint32("proto-version", protoVer).
			Int64("ms", time.Now().UnixMilli()).
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
			if err != nil {
				return fmt.Errorf("failed to receive continuation startup message: %w", err)
			}

			spqrlog.Zero.Info().
				Uint("client", cl.ID()).
				Uint32("proto-version", protoVer).
				Msg("completed TLS setup")

			switch msg := frsm.(type) {
			case *pgproto3.StartupMessage:
				sm = msg
			default:
				return fmt.Errorf("received unexpected message type %T", frsm)
			}
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

		if cl.DB() == pingRoute && cl.Usr() == pingRoute {
			return nil
		}

		cl.SetUsr(cl.Usr())
		cl.SetStartupParams(sm.Parameters)

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
				Severity: "ERROR",
				Message:  fmt.Sprintf("auth failed %s", err),
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

	/* XXX: generate defaults for virtual pool too */
	if cl.Rule().PoolMode != config.PoolModeVirtual {
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
		Msg("received message from client")
	return msg, err
}

func (cl *PsqlClient) Send(msg pgproto3.BackendMessage) error {
	spqrlog.Zero.Debug().
		Uint("client", cl.ID()).
		Type("msg-type", msg).
		Msg("sending msg to client")

	cl.be.Send(msg)

	switch msg.(type) {
	case *pgproto3.ReadyForQuery, *pgproto3.ErrorResponse, *pgproto3.AuthenticationCleartextPassword, *pgproto3.AuthenticationOk, *pgproto3.AuthenticationMD5Password, *pgproto3.AuthenticationGSS, *pgproto3.AuthenticationGSSContinue, *pgproto3.AuthenticationSASLFinal, *pgproto3.AuthenticationSASLContinue, *pgproto3.AuthenticationSASL, *pgproto3.CopyInResponse, *pgproto3.CopyOutResponse, *pgproto3.CopyBothResponse:
		return cl.be.Flush()
	default:
		return nil
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
	if cl.serverP.Load() != nil {
		return fmt.Errorf("client already has active connection")
	}
	cl.serverP.Store(&srv)
	return nil
}

func (cl *PsqlClient) SwitchServerConn(srv server.Server) error {
	cl.serverP.Store(&srv)
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
	spqrlog.Zero.Debug().Uint("client-id", cl.ID()).Msg("closing client")
	return cl.conn.Close()
}

func (cl *PsqlClient) replyErrMsgHint(
	msg string,
	code string,
	hint string, pos int32, s txstatus.TXStatus) error {
	var clErrMsg string

	if cl.ReplyClientId {
		clErrMsg = fmt.Sprintf("client %p: error %v", cl, msg)
	} else {
		clErrMsg = msg
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.ErrorResponse{
			Message:  clErrMsg,
			Severity: "ERROR",
			Code:     code,
			Hint:     hint,
			Position: pos,
		},
		&pgproto3.ReadyForQuery{
			TxStatus: byte(s),
		},
	} {
		if err := cl.Send(msg); err != nil {
			return err
		}
	}
	return nil
}

func (cl *PsqlClient) ReplyErrMsg(msg string, code string, pos int32, s txstatus.TXStatus) error {
	return cl.replyErrMsgHint(msg, code, "", pos, s)
}

func (cl *PsqlClient) ReplyErrWithTxStatus(e error, s txstatus.TXStatus) error {
	switch er := e.(type) {
	case *spqrerror.SpqrError:
		return cl.ReplyErrMsg(er.Error(), er.ErrorCode, er.Position, s)
	default:
		return cl.ReplyErrMsg(e.Error(), spqrerror.SPQR_UNEXPECTED, 0, s)
	}
}

func (cl *PsqlClient) ReplyErr(e error) error {

	switch er := e.(type) {
	case *spqrerror.SpqrError:
		return cl.replyErrMsgHint(er.Error(), er.ErrorCode, er.ErrHint, er.Position, txstatus.TXIDLE)
	default:
		return cl.ReplyErrMsg(e.Error(), spqrerror.SPQR_UNEXPECTED, 0, txstatus.TXIDLE)
	}
}

func (cl *PsqlClient) ReplyErrMsgByCode(code string) error {
	clErrMsg := spqrerror.GetMessageByCode(code)
	return cl.ReplyErrMsg(clErrMsg, code, 0, txstatus.TXIDLE)
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

func (f FakeClient) Rule() *config.FrontendRule {
	return &config.FrontendRule{
		Usr: DefaultUsr,
		DB:  DefaultDB,
	}
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
		shards: make([]shard.ShardHostInstance, len(clientInfo.Shards)),
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
	shards []shard.ShardHostInstance
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

func (c NoopClient) Shards() []shard.ShardHostInstance {
	return c.shards
}

func (c NoopClient) Conn() net.Conn {
	return nil
}

type MockShard struct {
	shard.ShardHostInstance

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
var _ shard.ShardHostInstance = &MockShard{}
var _ conn.DBInstance = &MockDBInstance{}
