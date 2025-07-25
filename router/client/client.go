package client

import (
	"bufio"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/prepstatement"
	"github.com/pg-sharding/spqr/pkg/tsa"
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
	"github.com/pg-sharding/spqr/router/twopc"
)

type RouterClient interface {
	client.Client
	prepstatement.PreparedStatementMapper

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
	beginTxParamSet   map[string]string
	localTxParamSet   map[string]string
	statementParamSet map[string]string
	activeParamSet    map[string]string

	savepointParamSet  map[string]map[string]string
	savepointTxCounter map[string]int

	/* cancel */
	csm *pgproto3.CancelRequest

	cancel_pid uint32
	cancel_key uint32

	txCnt         int
	ReplyClientId bool

	rule *config.FrontendRule
	conn conn.RawConn

	r *route.Route

	prepStmts     map[string]*prepstatement.PreparedStatementDefinition
	prepStmtsHash map[string]uint64

	/* target-session-attrs */
	defaultTsa string

	be *pgproto3.Backend

	startupMsg *pgproto3.StartupMessage

	bindParams [][]byte

	paramCodes []int16

	show_notice_messages bool
	maintain_params      bool

	id uint

	cacheCC pgproto3.CommandComplete

	serverP atomic.Pointer[server.Server]
}

var _ RouterClient = &PsqlClient{}

func (cl *PsqlClient) resolveVirtualBoolParam(name string, defaultVal bool) bool {
	if val, ok := cl.localTxParamSet[name]; ok {
		return val == "ok"
	}
	if val, ok := cl.statementParamSet[name]; ok {
		return val == "ok"
	}
	if val, ok := cl.activeParamSet[name]; ok {
		return val == "ok"
	}
	return defaultVal
}

func (cl *PsqlClient) recordVirtualParam(level string, name string, val string) {
	switch level {
	case session.VirtualParamLevelLocal:
		cl.localTxParamSet[name] = val
	case session.VirtualParamLevelStatement:
		cl.statementParamSet[name] = val
	default:
		cl.activeParamSet[name] = val
	}
}

func (cl *PsqlClient) resolveVirtualStringParam(name string) string {
	if val, ok := cl.localTxParamSet[name]; ok {
		return val
	}
	if val, ok := cl.statementParamSet[name]; ok {
		return val
	}
	if val, ok := cl.activeParamSet[name]; ok {
		return val
	}
	return ""
}

// SetDistribution implements RouterClient.
func (cl *PsqlClient) SetDistribution(level string, val string) {
	cl.recordVirtualParam(level, session.SPQR_DISTRIBUTION, val)
}

// Distribution implements RouterClient.
func (cl *PsqlClient) Distribution() string {
	return cl.resolveVirtualStringParam(session.SPQR_DISTRIBUTION)
}

// SetDistributedRelation implements RouterClient.
func (cl *PsqlClient) SetDistributedRelation(level string, val string) {
	cl.recordVirtualParam(level, session.SPQR_DISTRIBUTED_RELATION, val)
}

// DistributedRelation implements RouterClient.
func (cl *PsqlClient) DistributedRelation() string {
	return cl.resolveVirtualStringParam(session.SPQR_DISTRIBUTED_RELATION)
}

// SetExecuteOn implements RouterClient.
func (cl *PsqlClient) SetExecuteOn(level string, val string) {
	cl.recordVirtualParam(level, session.SPQR_EXECUTE_ON, val)
}

// ExecuteOn implements RouterClient.
func (cl *PsqlClient) ExecuteOn() string {
	return cl.resolveVirtualStringParam(session.SPQR_EXECUTE_ON)
}

// SetExecuteOn implements RouterClient.
func (cl *PsqlClient) SetEnhancedMultiShardProcessing(level string, val bool) {
	if val {
		cl.recordVirtualParam(level, session.SPQR_ENGINE_V2, "ok")
	} else {
		cl.recordVirtualParam(level, session.SPQR_ENGINE_V2, "no")
	}
}

// ExecuteOn implements RouterClient.
func (cl *PsqlClient) EnhancedMultiShardProcessing() bool {
	return cl.resolveVirtualBoolParam(session.SPQR_ENGINE_V2, config.RouterConfig().Qr.EnhancedMultiShardProcessing)
}

func (cl *PsqlClient) SetCommitStrategy(val string) {
	cl.recordVirtualParam(session.VirtualParamLevelTxBlock, session.SPQR_COMMIT_STRATEGY, val)
}

func (cl *PsqlClient) CommitStrategy() string {
	return cl.resolveVirtualStringParam(session.SPQR_COMMIT_STRATEGY)
}

// SetAutoDistribution implements RouterClient.
func (cl *PsqlClient) SetAutoDistribution(val string) {
	cl.recordVirtualParam(session.VirtualParamLevelStatement, session.SPQR_AUTO_DISTRIBUTION, val)
}

// AutoDistribution implements RouterClient.
func (cl *PsqlClient) AutoDistribution() string {
	return cl.resolveVirtualStringParam(session.SPQR_AUTO_DISTRIBUTION)
}

// SetDistributionKey implements RouterClient.
func (cl *PsqlClient) SetDistributionKey(val string) {
	cl.recordVirtualParam(session.VirtualParamLevelStatement, session.SPQR_DISTRIBUTION_KEY, val)
}

// DistributionKey implements RouterClient.
func (cl *PsqlClient) DistributionKey() string {
	return cl.resolveVirtualStringParam(session.SPQR_DISTRIBUTION_KEY)
}

// MaintainParams implements RouterClient.
func (cl *PsqlClient) MaintainParams() bool {
	return cl.maintain_params
}

// SetMaintainParams implements RouterClient.
func (cl *PsqlClient) SetMaintainParams(level string, val bool) {
	cl.maintain_params = val
}

// SetShowNoticeMsg implements client.Client.
func (cl *PsqlClient) SetShowNoticeMsg(level string, val bool) {
	cl.show_notice_messages = val
}

// ShowNoticeMsg implements RouterClient.
func (cl *PsqlClient) ShowNoticeMsg() bool {
	return cl.show_notice_messages
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
func (cl *PsqlClient) SetShardingKey(level string, k string) {
	cl.recordVirtualParam(level, session.SPQR_SHARDING_KEY, k)
}

// ShardingKey implements RouterClient.
func (cl *PsqlClient) ShardingKey() string {
	return cl.resolveVirtualStringParam(session.SPQR_SHARDING_KEY)
}

// SetDefaultRouteBehaviour implements RouterClient.
func (cl *PsqlClient) SetDefaultRouteBehaviour(level string, b string) {
	cl.recordVirtualParam(level, session.SPQR_DEFAULT_ROUTE_BEHAVIOUR, b)
}

// DefaultRouteBehaviour implements RouterClient.
func (cl *PsqlClient) DefaultRouteBehaviour() string {
	return cl.resolveVirtualStringParam(session.SPQR_DEFAULT_ROUTE_BEHAVIOUR)
}

// ScatterQuery implements RouterClient.
func (cl *PsqlClient) ScatterQuery() bool {
	return cl.resolveVirtualBoolParam(session.SPQR_SCATTER_QUERY, false)
}

// SetScatterQuery implements RouterClient.
func (cl *PsqlClient) SetScatterQuery(val bool) {
	if val {
		cl.recordVirtualParam(session.VirtualParamLevelStatement, session.SPQR_SCATTER_QUERY, "ok")
	} else {
		cl.recordVirtualParam(session.VirtualParamLevelStatement, session.SPQR_SCATTER_QUERY, "no")
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

	cl := &PsqlClient{
		activeParamSet: map[string]string{
			session.SPQR_DISTRIBUTION:            "default",
			session.SPQR_DEFAULT_ROUTE_BEHAVIOUR: defaultRouteBehaviour,
		},
		statementParamSet: map[string]string{},
		localTxParamSet:   map[string]string{},
		conn:              pgconn,
		startupMsg:        &pgproto3.StartupMessage{},
		prepStmts:         map[string]*prepstatement.PreparedStatementDefinition{},
		prepStmtsHash:     map[string]uint64{},
		defaultTsa:        target_session_attrs,

		show_notice_messages: showNoticeMessages,

		serverP: atomic.Pointer[server.Server]{},
	}

	cl.SetCommitStrategy(twopc.COMMIT_STRATEGY_BEST_EFFORT)

	cl.id = spqrlog.GetPointer(cl)

	cl.serverP.Store(nil)

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
	cl.savepointTxCounter = nil
	cl.statementParamSet = map[string]string{}
	cl.localTxParamSet = map[string]string{}
	cl.txCnt = 0
}

func (cl *PsqlClient) CommitActiveSet() {
	cl.beginTxParamSet = nil
	cl.savepointParamSet = nil
	cl.savepointTxCounter = nil
	cl.statementParamSet = map[string]string{}
	cl.localTxParamSet = map[string]string{}
	cl.txCnt = 0
}

func (cl *PsqlClient) CleanupStatementSet() {
	cl.statementParamSet = map[string]string{}
}

func (cl *PsqlClient) Savepoint(name string) {
	cl.savepointParamSet[name] = copymap(cl.activeParamSet)
	cl.savepointTxCounter[name] = cl.txCnt
	cl.txCnt++
}

func (cl *PsqlClient) Rollback() {
	cl.activeParamSet = copymap(cl.beginTxParamSet)
	cl.beginTxParamSet = nil
	cl.savepointParamSet = nil
	cl.savepointTxCounter = nil
	cl.statementParamSet = map[string]string{}
	cl.localTxParamSet = map[string]string{}

	cl.txCnt = 0
}

func (cl *PsqlClient) RollbackToSP(name string) {
	cl.activeParamSet = cl.savepointParamSet[name]
	targetTxCnt := cl.savepointTxCounter[name]
	for k := range cl.savepointParamSet {
		if cl.savepointTxCounter[k] > targetTxCnt {
			delete(cl.savepointTxCounter, k)
			delete(cl.savepointParamSet, k)
		}
	}
	/* XXX: not exactly correct with roolback to SP */
	cl.statementParamSet = map[string]string{}
	/* XXX: not exactly correct with roolback to SP */
	cl.localTxParamSet = map[string]string{}

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

		if len(k) >= 6 && k[0:6] == "__spqr" {
			continue
		}

		query.String += fmt.Sprintf("SET %s='%s';", k, v)
	}

	return query
}

func (cl *PsqlClient) ResetAll() {
	cl.activeParamSet = cl.startupMsg.Parameters
	cl.statementParamSet = map[string]string{}
	cl.localTxParamSet = map[string]string{}
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
		Msgf("sending msg to client")

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

func (cl *PsqlClient) Params() map[string]string {
	return cl.activeParamSet
}

func (cl *PsqlClient) replyErrMsgHint(msg string, code string, hint string, s txstatus.TXStatus) error {
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

func (cl *PsqlClient) ReplyErrMsg(msg string, code string, s txstatus.TXStatus) error {
	return cl.replyErrMsgHint(msg, code, "", s)
}

func (cl *PsqlClient) ReplyErrWithTxStatus(e error, s txstatus.TXStatus) error {
	switch er := e.(type) {
	case *spqrerror.SpqrError:
		return cl.ReplyErrMsg(er.Error(), er.ErrorCode, s)
	default:
		return cl.ReplyErrMsg(e.Error(), spqrerror.SPQR_UNEXPECTED, s)
	}
}

func (cl *PsqlClient) ReplyErr(e error) error {
	switch er := e.(type) {
	case *spqrerror.SpqrError:
		return cl.replyErrMsgHint(er.Error(), er.ErrorCode, er.ErrHint, txstatus.TXIDLE)
	default:
		return cl.ReplyErrMsg(e.Error(), spqrerror.SPQR_UNEXPECTED, txstatus.TXIDLE)
	}
}

func (cl *PsqlClient) ReplyErrMsgByCode(code string) error {
	clErrMsg := spqrerror.GetMessageByCode(code)
	return cl.ReplyErrMsg(clErrMsg, code, txstatus.TXIDLE)
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

func (cl *PsqlClient) GetTsa() tsa.TSA {
	if _, ok := cl.statementParamSet[session.SPQR_TARGET_SESSION_ATTRS]; !ok {
		if _, ok := cl.activeParamSet[session.SPQR_TARGET_SESSION_ATTRS]; !ok {
			return tsa.TSA(cl.defaultTsa)
		}
	}
	return tsa.TSA(cl.resolveVirtualStringParam(session.SPQR_TARGET_SESSION_ATTRS))
}

func (cl *PsqlClient) SetTsa(level string, s string) {
	switch s {
	case config.TargetSessionAttrsAny,
		config.TargetSessionAttrsPS,
		config.TargetSessionAttrsRW,
		config.TargetSessionAttrsSmartRW,
		config.TargetSessionAttrsRO:
		cl.recordVirtualParam(level, session.SPQR_TARGET_SESSION_ATTRS, s)
	default:
		// XXX: else error out!
	}
}

func (cl *PsqlClient) ResetTsa() {
	cl.SetTsa(session.VirtualParamLevelTxBlock, cl.defaultTsa)
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
