package relay

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"time"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/prepstatement"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/opentracing/opentracing-go"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/pg-sharding/spqr/router/client"
	"github.com/pg-sharding/spqr/router/parser"
	"github.com/pg-sharding/spqr/router/plan"
	"github.com/pg-sharding/spqr/router/poolmgr"
	"github.com/pg-sharding/spqr/router/qrouter"
	"github.com/pg-sharding/spqr/router/route"
	"github.com/pg-sharding/spqr/router/server"
	"github.com/pg-sharding/spqr/router/statistics"
	"golang.org/x/exp/slices"
)

type RelayStateMgr interface {
	poolmgr.ConnectionKeeper
	route.RouteMgr

	QueryExecutor() QueryStateExecutor
	QueryRouter() qrouter.QueryRouter
	PoolMgr() poolmgr.PoolMgr

	Reset() error
	Flush()

	Parse(query string, doCaching bool) (parser.ParseState, string, error)

	AddQuery(q pgproto3.FrontendMessage)
	AddSilentQuery(q pgproto3.FrontendMessage)

	RelayStep(msg pgproto3.FrontendMessage, waitForResp bool, replyCl bool) ([]pgproto3.BackendMessage, error)

	CompleteRelay(replyCl bool) error
	Close() error
	Client() client.RouterClient

	PrepareStatement(hash uint64, d *prepstatement.PreparedStatementDefinition) (*prepstatement.PreparedStatementDescriptor, pgproto3.BackendMessage, error)

	PrepareRelayStep() (plan.Plan, error)
	PrepareRelayStepOnAnyRoute() (func() error, error)
	PrepareRelayStepOnHintRoute(route *kr.ShardKey) error

	HoldRouting()
	UnholdRouting()

	/* process extended proto */
	ProcessMessage(msg pgproto3.FrontendMessage, waitForResp, replyCl bool) error
	ProcessMessageBuf(waitForResp, replyCl bool) error

	AddExtendedProtocMessage(q pgproto3.FrontendMessage)
	ProcessExtendedBuffer() error
}

type BufferedMessageType int

const (
	// Message from client
	BufferedMessageRegular = BufferedMessageType(0)
	// Message produced by spqr
	BufferedMessageInternal = BufferedMessageType(1)
)

type BufferedMessage struct {
	msg pgproto3.FrontendMessage

	tp BufferedMessageType
}

func RegularBufferedMessage(q pgproto3.FrontendMessage) BufferedMessage {
	return BufferedMessage{
		msg: q,
		tp:  BufferedMessageRegular,
	}
}

func InternalBufferedMessage(q pgproto3.FrontendMessage) BufferedMessage {
	return BufferedMessage{
		msg: q,
		tp:  BufferedMessageInternal,
	}
}

type PortalDesc struct {
	rd     *pgproto3.RowDescription
	nodata *pgproto3.NoData
}

type ParseCacheEntry struct {
	ps   parser.ParseState
	comm string
	stmt lyx.Node
}

type RelayStateImpl struct {
	traceMsgs    bool
	activeShards []kr.ShardKey

	routingState plan.Plan

	Qr      qrouter.QueryRouter
	qse     QueryStateExecutor
	qp      parser.QParser
	plainQ  string
	Cl      client.RouterClient
	poolMgr poolmgr.PoolMgr

	msgBuf []BufferedMessage

	holdRouting bool

	bindRoute    *kr.ShardKey
	lastBindName string

	execute func() error

	saveBind        *pgproto3.Bind
	savedPortalDesc map[string]PortalDesc

	parseCache map[string]ParseCacheEntry

	// buffer of messages to process on Sync request
	xBuf []pgproto3.FrontendMessage
}

// SetTxStatus implements poolmgr.ConnectionKeeper.
func (rst *RelayStateImpl) SetTxStatus(status txstatus.TXStatus) {
	rst.qse.SetTxStatus(status)
}

// TxStatus implements poolmgr.ConnectionKeeper.
func (rst *RelayStateImpl) TxStatus() txstatus.TXStatus {
	return rst.qse.TxStatus()
}

// HoldRouting implements RelayStateMgr.
func (rst *RelayStateImpl) HoldRouting() {
	rst.holdRouting = true
}

// UnholdRouting implements RelayStateMgr.
func (rst *RelayStateImpl) UnholdRouting() {
	rst.holdRouting = false
}

func NewRelayState(qr qrouter.QueryRouter, client client.RouterClient, manager poolmgr.PoolMgr) RelayStateMgr {
	return &RelayStateImpl{
		activeShards:    nil,
		msgBuf:          nil,
		traceMsgs:       false,
		qse:             NewQueryStateExecutor(client),
		Qr:              qr,
		Cl:              client,
		poolMgr:         manager,
		execute:         nil,
		savedPortalDesc: map[string]PortalDesc{},
		parseCache:      map[string]ParseCacheEntry{},
	}
}

func (rst *RelayStateImpl) SyncCount() int64 {
	server := rst.Client().Server()
	if server == nil {
		return 0
	}
	return server.Sync()
}

func (rst *RelayStateImpl) DataPending() bool {
	server := rst.Client().Server()

	if server == nil {
		return false
	}

	return server.DataPending()
}

func (rst *RelayStateImpl) QueryRouter() qrouter.QueryRouter {
	return rst.Qr
}

func (rst *RelayStateImpl) QueryExecutor() QueryStateExecutor {
	return rst.qse
}

func (rst *RelayStateImpl) PoolMgr() poolmgr.PoolMgr {
	return rst.poolMgr
}

func (rst *RelayStateImpl) Client() client.RouterClient {
	return rst.Cl
}

// TODO : unit tests
func (rst *RelayStateImpl) PrepareStatement(hash uint64, d *prepstatement.PreparedStatementDefinition) (*prepstatement.PreparedStatementDescriptor, pgproto3.BackendMessage, error) {
	serv := rst.Client().Server()

	shards := serv.Datashards()
	if len(shards) == 0 {
		return nil, nil, spqrerror.New(spqrerror.SPQR_NO_DATASHARD, "No active shards")
	}
	shardId := shards[0].ID()

	if ok, rd := serv.HasPrepareStatement(hash, shardId); ok {
		return rd, &pgproto3.ParseComplete{}, nil
	}

	// Do not wait for result
	// simply fire backend msg
	if err := serv.Send(&pgproto3.Parse{
		Name:          d.Name,
		Query:         d.Query,
		ParameterOIDs: d.ParameterOIDs,
	}); err != nil {
		return nil, nil, err
	}

	err := serv.Send(&pgproto3.Describe{
		ObjectType: 'S',
		Name:       d.Name,
	})
	if err != nil {
		return nil, nil, err
	}

	spqrlog.Zero.Debug().Uint("client", rst.Client().ID()).Msg("syncing connection")

	unreplied, err := rst.RelayStep(&pgproto3.Sync{}, true, false)
	if err != nil {
		return nil, nil, err
	}

	rd := &prepstatement.PreparedStatementDescriptor{
		NoData:    false,
		RowDesc:   nil,
		ParamDesc: nil,
	}

	var retMsg pgproto3.BackendMessage

	deployed := false

	for _, msg := range unreplied {
		spqrlog.Zero.Debug().Uint("client", rst.Client().ID()).Interface("type", msg).Msg("unreplied msg in prepare")
		switch q := msg.(type) {
		case *pgproto3.ParseComplete:
			// skip
			retMsg = msg
			deployed = true
		case *pgproto3.ErrorResponse:
			retMsg = msg
		case *pgproto3.NoData:
			rd.NoData = true
		case *pgproto3.ParameterDescription:
			// copy
			cp := *q
			rd.ParamDesc = &cp
		case *pgproto3.RowDescription:
			// copy
			rd.RowDesc = &pgproto3.RowDescription{}

			rd.RowDesc.Fields = make([]pgproto3.FieldDescription, len(q.Fields))

			for i := range len(q.Fields) {
				s := make([]byte, len(q.Fields[i].Name))
				copy(s, q.Fields[i].Name)

				rd.RowDesc.Fields[i] = q.Fields[i]
				rd.RowDesc.Fields[i].Name = s
			}
		default:
		}
	}

	if deployed {
		// don't need to complete relay because tx state didn't change
		if err := rst.Cl.Server().StorePrepareStatement(hash, shardId, d, rd); err != nil {
			return nil, nil, err
		}
	}
	return rd, retMsg, nil
}

func (rst *RelayStateImpl) multishardPrepareDDL(hash uint64, d *prepstatement.PreparedStatementDefinition) error {
	serv := rst.Client().Server()

	shards := serv.Datashards()
	if len(shards) == 0 {
		return spqrerror.New(spqrerror.SPQR_NO_DATASHARD, "No active shards")
	}

	for _, shard := range shards {
		shardId := shard.ID()
		shKey := shard.SHKey()

		if ok, _ := serv.HasPrepareStatement(hash, shardId); ok {
			continue
		}

		if err := serv.SendShard(&pgproto3.Parse{
			Name:          d.Name,
			Query:         d.Query,
			ParameterOIDs: d.ParameterOIDs,
		}, &shKey); err != nil {
			return err
		}

		if err := serv.SendShard(&pgproto3.Describe{
			ObjectType: byte('S'),
			Name:       d.Name,
		}, &shKey); err != nil {
			return err
		}

		if err := serv.SendShard(&pgproto3.Sync{}, &shKey); err != nil {
			return err
		}

		rd := &prepstatement.PreparedStatementDescriptor{
			NoData:    false,
			RowDesc:   nil,
			ParamDesc: nil,
		}
		parsed := false
		finished := false
		for !finished {
			msg, err := serv.ReceiveShard(shardId)
			if err != nil {
				return err
			}
			switch q := msg.(type) {
			case *pgproto3.ParseComplete:
				parsed = true
			case *pgproto3.ReadyForQuery:
				finished = true
			case *pgproto3.ErrorResponse:
				return fmt.Errorf("error preparing DDL statement: \"%s\"", q.Message)
			case *pgproto3.NoData:
				rd.NoData = true
			case *pgproto3.ParameterDescription:
				// copy
				cp := *q
				rd.ParamDesc = &cp
			case *pgproto3.RowDescription:
				// copy
				rd.RowDesc = &pgproto3.RowDescription{}

				rd.RowDesc.Fields = make([]pgproto3.FieldDescription, len(q.Fields))

				for i := range len(q.Fields) {
					s := make([]byte, len(q.Fields[i].Name))
					copy(s, q.Fields[i].Name)

					rd.RowDesc.Fields[i] = q.Fields[i]
					rd.RowDesc.Fields[i].Name = s
				}
			default:
				return fmt.Errorf("received unexpected message type %T", msg)
			}
		}

		if !parsed {
			return fmt.Errorf("statement parsing not finished")
		}
		if err := serv.StorePrepareStatement(hash, shardId, d, rd); err != nil {
			return err
		}
	}
	return nil
}

func (rst *RelayStateImpl) multishardDescribePortal(bind *pgproto3.Bind) (*prepstatement.PreparedStatementDescriptor, error) {
	serv := rst.Client().Server()

	shards := serv.Datashards()
	if len(shards) == 0 {
		return nil, spqrerror.New(spqrerror.SPQR_NO_DATASHARD, "No active shards")
	}

	shard := shards[0]
	shardId := shard.ID()
	shkey := shard.SHKey()

	if err := serv.SendShard(bind, &shkey); err != nil {
		return nil, err
	}

	if err := serv.SendShard(&pgproto3.Describe{
		ObjectType: byte('P'),
	}, &shkey); err != nil {
		return nil, err
	}

	if err := serv.SendShard(&pgproto3.Close{ObjectType: byte('P')}, &shkey); err != nil {
		return nil, err
	}

	if err := serv.SendShard(&pgproto3.Sync{}, &shkey); err != nil {
		return nil, err
	}

	rd := &prepstatement.PreparedStatementDescriptor{
		NoData:    false,
		RowDesc:   nil,
		ParamDesc: nil,
	}
	var saveCloseComplete *pgproto3.CloseComplete
	finished := false
	for !finished {
		msg, err := serv.ReceiveShard(shardId)
		if err != nil {
			return nil, err
		}
		switch q := msg.(type) {
		case *pgproto3.BindComplete:
			// that's ok
			continue
		case *pgproto3.ReadyForQuery:
			finished = true
		case *pgproto3.ErrorResponse:
			return nil, fmt.Errorf("error describing DDL portal: \"%s\"", q.Message)
		case *pgproto3.NoData:
			rd.NoData = true
		case *pgproto3.CloseComplete:
			saveCloseComplete = q
			continue
		case *pgproto3.RowDescription:
			// copy
			rd.RowDesc = &pgproto3.RowDescription{}

			rd.RowDesc.Fields = make([]pgproto3.FieldDescription, len(q.Fields))

			for i := range len(q.Fields) {
				s := make([]byte, len(q.Fields[i].Name))
				copy(s, q.Fields[i].Name)

				rd.RowDesc.Fields[i] = q.Fields[i]
				rd.RowDesc.Fields[i].Name = s
			}
		default:
			return nil, fmt.Errorf("received unexpected message type %T", msg)
		}
	}

	if saveCloseComplete == nil {
		return nil, fmt.Errorf("portal was not closed after describe")
	}

	return rd, nil
}

func (rst *RelayStateImpl) Close() error {
	defer func() {
		if err := rst.Cl.Close(); err != nil {
			spqrlog.Zero.Debug().Err(err).Msg("failed to close client connection")
		}
	}()
	defer rst.ActiveShardsReset()
	return rst.poolMgr.UnRouteCB(rst.Cl, rst.activeShards)
}

func (rst *RelayStateImpl) ActiveShardsReset() {
	rst.activeShards = nil
}

func (rst *RelayStateImpl) ActiveShards() []kr.ShardKey {
	return rst.activeShards
}

// TODO : unit tests
func (rst *RelayStateImpl) Reset() error {
	rst.activeShards = nil
	rst.qse.SetTxStatus(txstatus.TXIDLE)

	_ = rst.Cl.Reset()

	return rst.Cl.Unroute()
}

func (rst *RelayStateImpl) StartTrace() {
	rst.traceMsgs = true
}

// TODO : unit tests
func (rst *RelayStateImpl) Flush() {
	rst.msgBuf = nil
	rst.traceMsgs = false
}

var ErrSkipQuery = fmt.Errorf("wait for a next query")
var ErrMatchShardError = fmt.Errorf("failed to match datashard")

// TODO : unit tests
func (rst *RelayStateImpl) procRoutes(routes []*kr.ShardKey) error {
	// if there is no routes configured, there is nowhere to route to
	if len(routes) == 0 {
		return ErrMatchShardError
	}

	spqrlog.Zero.Debug().
		Uint("relay state", spqrlog.GetPointer(rst)).
		Msg("unroute previous connections")

	if err := rst.Unroute(rst.activeShards); err != nil {
		return err
	}

	rst.activeShards = nil
	for _, shr := range routes {
		rst.activeShards = append(rst.activeShards, *shr)
	}

	if config.RouterConfig().PgprotoDebug {
		if err := rst.Cl.ReplyDebugNoticef("matched datashard routes %+v", routes); err != nil {
			return err
		}
	}

	if err := rst.Connect(); err != nil {
		spqrlog.Zero.Error().
			Err(err).
			Uint("client", rst.Client().ID()).
			Msg("client encounter while initialing server connection")
		return err
	}

	/* if transaction is explicitly requested, deploy */
	if err := rst.QueryExecutor().Deploy(rst.Client().Server()); err != nil {
		return err
	}

	/* take care of session param if we told to */
	if rst.Client().MaintainParams() {
		query := rst.Cl.ConstructClientParams()
		spqrlog.Zero.Debug().
			Uint("client", rst.Client().ID()).
			Str("query", query.String).
			Msg("setting params for client")
		_, err := rst.qse.ProcQuery(&QueryDesc{
			Msg:  query,
			Stmt: rst.qp.Stmt(),
			P:    nil,
		}, rst.Qr.Mgr(), true, false)
		return err
	}

	return nil
}

// TODO : unit tests
func (rst *RelayStateImpl) expandRoutes(routes []*kr.ShardKey) error {
	// if there is no routes to expand, there is nowhere to do
	if len(routes) == 0 {
		return nil
	}

	if rst.Client().Server().TxStatus() == txstatus.TXERR {
		/* should never happen */
		return fmt.Errorf("unexpected server expand request")
	}

	_ = rst.Client().SwitchServerConn(rst.Client().Server().ToMultishard())

	beforeTx := rst.Client().Server().TxStatus()

	for _, shkey := range routes {
		if slices.ContainsFunc(rst.activeShards, func(c kr.ShardKey) bool {
			return *shkey == c
		}) {
			continue
		}

		rst.activeShards = append(rst.activeShards, *shkey)

		spqrlog.Zero.Debug().
			Str("client tsa", string(rst.Client().GetTsa())).
			Str("deploying tx", beforeTx.String()).
			Msg("expanding shard with tsa")

		if err := rst.Client().Server().ExpandDataShard(rst.Client().ID(), *shkey, rst.Client().GetTsa(), beforeTx == txstatus.TXACT); err != nil {
			return err
		}
	}

	/* take care of session param if we told to */
	/* TODO: fix */
	// if rst.Client().MaintainParams() {
	// 	query := rst.Cl.ConstructClientParams()
	// 	spqrlog.Zero.Debug().
	// 		Uint("client", rst.Client().ID()).
	// 		Str("query", query.String).
	// 		Msg("setting params for client")
	// 	_, err := rst.qse.ProcQuery(query, rst.qp.Stmt(), rst.Qr.Mgr(), true, false)
	// 	return err
	// }

	return nil
}

func (rst *RelayStateImpl) selectRandomRoute() (*kr.ShardKey, error) {
	routes := rst.Qr.DataShardsRoutes()
	if len(routes) == 0 {
		return nil, fmt.Errorf("no routes configured")
	}

	r := routes[rand.Int()%len(routes)]
	rst.routingState = plan.ShardDispatchPlan{
		ExecTarget: r,
	}

	return r, nil
}

// TODO : unit tests
func (rst *RelayStateImpl) Reroute() ([]*kr.ShardKey, plan.Plan, error) {
	_ = rst.Cl.ReplyDebugNotice("rerouting the client connection")

	span := opentracing.StartSpan("reroute")
	defer span.Finish()
	span.SetTag("user", rst.Cl.Usr())
	span.SetTag("db", rst.Cl.DB())

	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Str("drb", rst.Client().DefaultRouteBehaviour()).
		Str("exec_on", rst.Client().ExecuteOn()).
		Msg("rerouting the client connection, resolving shard")

	var queryPlan plan.Plan

	if v := rst.Client().ExecuteOn(); v != "" {
		queryPlan = plan.ShardDispatchPlan{
			ExecTarget: &kr.ShardKey{
				Name: v,
			},
		}
	} else {
		var err error
		queryPlan, err = rst.Qr.Route(context.TODO(), rst.qp.Stmt(), rst.Cl)
		if err != nil {
			return nil, nil, fmt.Errorf("error processing query '%v': %v", rst.plainQ, err)
		}
	}

	rst.routingState = queryPlan

	switch v := queryPlan.(type) {
	case plan.VirtualPlan:
		/* XXX: connect to routes in subplan */
		return nil, queryPlan, nil
	case plan.ScatterPlan:
		spqrlog.Zero.Debug().
			Uint("client", rst.Client().ID()).
			Msgf("parsed ScatterPlan")
		return rst.Qr.DataShardsRoutes(), queryPlan, nil
	case plan.DDLState:
		spqrlog.Zero.Debug().
			Uint("client", rst.Client().ID()).
			Msgf("parsed DDLState")
		return rst.Qr.DataShardsRoutes(), queryPlan, nil
	case plan.ShardDispatchPlan:
		// TBD: do it better
		return []*kr.ShardKey{v.ExecTarget}, queryPlan, nil
	case plan.RandomDispatchPlan:
		r, err := rst.selectRandomRoute()
		if err != nil {
			return nil, nil, err
		}

		return []*kr.ShardKey{r}, queryPlan, nil
	default:
		return nil, nil, fmt.Errorf("unexpected query plan %T", v)
	}
}

// TODO : unit tests
func (rst *RelayStateImpl) RerouteToRandomRoute() error {
	_ = rst.Cl.ReplyDebugNotice("rerouting the client connection")

	span := opentracing.StartSpan("reroute")
	defer span.Finish()
	span.SetTag("user", rst.Cl.Usr())
	span.SetTag("db", rst.Cl.DB())

	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Msg("rerouting the client connection to random shard, resolving shard")

	r, err := rst.selectRandomRoute()
	if err != nil {
		return err
	}
	return rst.procRoutes([]*kr.ShardKey{r})
}

// TODO : unit tests
func (rst *RelayStateImpl) RerouteToTargetRoute(route *kr.ShardKey) error {
	_ = rst.Cl.ReplyDebugNotice("rerouting the client connection")

	span := opentracing.StartSpan("reroute")
	defer span.Finish()
	span.SetTag("user", rst.Cl.Usr())
	span.SetTag("db", rst.Cl.DB())

	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Interface("statement", rst.qp.Stmt()).
		Msg("rerouting the client connection to target shard, resolving shard")

	rst.routingState = plan.ShardDispatchPlan{
		ExecTarget: route,
	}

	return rst.procRoutes([]*kr.ShardKey{route})
}

// TODO : unit tests
func (rst *RelayStateImpl) CurrentRoutes() []kr.ShardKey {
	switch q := rst.routingState.(type) {
	case plan.ShardDispatchPlan:
		return []kr.ShardKey{*q.ExecTarget}
	default:
		return nil
	}
}

// TODO : unit tests
func replyShardMatches(client client.RouterClient, sh []*kr.ShardKey) error {
	var shardNames []string
	for _, shkey := range sh {
		shardNames = append(shardNames, shkey.Name)
	}
	sort.Strings(shardNames)
	shardMatches := strings.Join(shardNames, ",")

	return client.ReplyNotice("send query to shard(s) : " + shardMatches)
}

// TODO : unit tests
func (rst *RelayStateImpl) Connect() error {
	var serv server.Server
	var err error

	if len(rst.ActiveShards()) > 1 {
		serv, err = server.NewMultiShardServer(rst.Cl.Route().ServPool())
		if err != nil {
			return err
		}
	} else {
		_ = rst.Cl.ReplyDebugNotice("open a connection to the single data shard")
		serv = server.NewShardServer(rst.Cl.Route().ServPool())
	}

	if err := rst.Cl.AssignServerConn(serv); err != nil {
		return err
	}

	spqrlog.Zero.Debug().
		Str("user", rst.Cl.Usr()).
		Str("db", rst.Cl.DB()).
		Uint("client", rst.Client().ID()).
		Msg("connect client to datashard routes")

	for _, shkey := range rst.ActiveShards() {
		spqrlog.Zero.Debug().
			Str("client tsa", string(rst.Client().GetTsa())).
			Msg("adding shard with tsa")
		if err := rst.Client().Server().AddDataShard(rst.Client().ID(), shkey, rst.Client().GetTsa()); err != nil {
			return err
		}
	}

	return nil
}

// TODO : unit tests
func (rst *RelayStateImpl) RelayFlush(waitForResp bool, replyCl bool) ([]pgproto3.BackendMessage, error) {
	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Msg("flushing message buffer")

	var unreplied []pgproto3.BackendMessage

	flusher := func(buff []BufferedMessage, waitForResp, replyCl bool) error {
		for len(buff) > 0 {
			var v BufferedMessage
			v, buff = buff[0], buff[1:]
			spqrlog.Zero.Debug().
				Bool("waitForResp", waitForResp).
				Bool("replyCl", replyCl).
				Msg("flushing")

			resolvedReplyCl := replyCl

			switch v.tp {
			case BufferedMessageInternal:
				resolvedReplyCl = false
			}

			if unrep_local, err := rst.qse.ProcQuery(
				&QueryDesc{
					Msg:  v.msg,
					Stmt: rst.qp.Stmt(),
					P:    rst.routingState, /*  ugh... fix this someday */
				}, rst.Qr.Mgr(), waitForResp, resolvedReplyCl); err != nil {
				return err
			} else {
				unreplied = append(unreplied, unrep_local...)
			}

		}

		return nil
	}
	buf := rst.msgBuf
	rst.msgBuf = nil

	if err := flusher(buf, waitForResp, replyCl); err != nil {
		return nil, err
	}

	return unreplied, nil
}

// TODO : unit tests
func (rst *RelayStateImpl) RelayStep(msg pgproto3.FrontendMessage, waitForResp bool, replyCl bool) ([]pgproto3.BackendMessage, error) {
	rst.AddQuery(msg)
	return rst.RelayFlush(waitForResp, replyCl)
}

func (rst *RelayStateImpl) CompleteRelay(replyCl bool) error {
	statistics.RecordFinishedTransaction(time.Now(), rst.Client().ID())

	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Str("txstatus", rst.qse.TxStatus().String()).
		Msg("complete relay iter")

	/* move this logic to executor */
	switch rst.qse.TxStatus() {
	case txstatus.TXIDLE:
		if replyCl {
			if err := rst.Cl.Send(&pgproto3.ReadyForQuery{
				TxStatus: byte(rst.qse.TxStatus()),
			}); err != nil {
				return err
			}
		}

		return rst.poolMgr.TXEndCB(rst)
	case txstatus.TXERR:
		fallthrough
	case txstatus.TXACT:
		if replyCl {
			if err := rst.Cl.Send(&pgproto3.ReadyForQuery{
				TxStatus: byte(rst.qse.TxStatus()),
			}); err != nil {
				return err
			}
		}
		/* preserve same route. Do not unroute */
		return nil
	default:
		_ = rst.Unroute(rst.activeShards)
		return fmt.Errorf("unknown tx status %v", rst.qse.TxStatus())
	}
}

// TODO : unit tests
func (rst *RelayStateImpl) Unroute(shkey []kr.ShardKey) error {
	newActiveShards := make([]kr.ShardKey, 0)
	for _, el := range rst.activeShards {
		if slices.IndexFunc(shkey, func(k kr.ShardKey) bool {
			return k == el
		}) == -1 {
			newActiveShards = append(newActiveShards, el)
		}
	}
	if err := rst.poolMgr.UnRouteCB(rst.Cl, shkey); err != nil {
		return err
	}
	if len(newActiveShards) > 0 {
		rst.activeShards = newActiveShards
	} else {
		rst.activeShards = nil
	}

	return nil
}

// TODO : unit tests
func (rst *RelayStateImpl) UnRouteWithError(shkey []kr.ShardKey, errmsg error) error {
	_ = rst.poolMgr.UnRouteWithError(rst.Cl, shkey, errmsg)
	return rst.Reset()
}

// TODO : unit tests
func (rst *RelayStateImpl) AddQuery(q pgproto3.FrontendMessage) {
	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Type("message-type", q).
		Msg("client relay: adding message to message buffer")
	rst.msgBuf = append(rst.msgBuf, RegularBufferedMessage(q))
}

// TODO : unit tests
func (rst *RelayStateImpl) AddSilentQuery(q pgproto3.FrontendMessage) {
	spqrlog.Zero.Debug().
		Interface("query", q).
		Msg("adding silent query")
	rst.msgBuf = append(rst.msgBuf, InternalBufferedMessage(q))
}

// TODO : unit tests
func (rst *RelayStateImpl) AddExtendedProtocMessage(q pgproto3.FrontendMessage) {
	spqrlog.Zero.Debug().
		Interface("query", q).
		Msg("adding extended protocol message")
	rst.xBuf = append(rst.xBuf, q)
}

// TODO : unit tests
func (rst *RelayStateImpl) DeployPrepStmt(qname string) (*prepstatement.PreparedStatementDescriptor, pgproto3.BackendMessage, error) {
	def := rst.Client().PreparedStatementDefinitionByName(qname)
	hash := rst.Client().PreparedStatementQueryHashByName(qname)

	server := rst.Client().Server()
	if server == nil {
		return nil, nil, fmt.Errorf("relay is not attached to deploy")
	}

	if len(server.Datashards()) != 1 {
		return nil, nil, fmt.Errorf("multishard prepared statement deploy is not supported")
	}

	spqrlog.Zero.Debug().
		Str("name", qname).
		Str("query", def.Query).
		Uint64("hash", hash).
		Uint("client", rst.Client().ID()).
		Uints("shards", shard.ShardIDs(rst.Client().Server().Datashards())).
		Msg("deploy prepared statement")

	name := fmt.Sprintf("%d", hash)
	return rst.PrepareStatement(hash, &prepstatement.PreparedStatementDefinition{
		Name:          name,
		Query:         def.Query,
		ParameterOIDs: def.ParameterOIDs,
	})
}

// TODO : unit tests
func (rst *RelayStateImpl) ProcessExtendedBuffer() error {

	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Int("xBuf", len(rst.xBuf)).
		Msg("process extended buffer")

	defer func() {
		// cleanup
		rst.xBuf = nil
		rst.bindRoute = nil
	}()

	holdRoute := true

	anyPrepStmt := ""
	for _, msg := range rst.xBuf {
		switch q := msg.(type) {
		case *pgproto3.Bind:
			if anyPrepStmt == "" {
				anyPrepStmt = q.PreparedStatement
			} else if anyPrepStmt != q.PreparedStatement {
				holdRoute = false
			}
		}
	}

	if holdRoute {
		defer rst.UnholdRouting()
	}

	for _, msg := range rst.xBuf {

	singleMsgLoop:

		switch q := msg.(type) {
		case *pgproto3.Parse:

			rst.Client().StorePreparedStatement(&prepstatement.PreparedStatementDefinition{
				Name:          q.Name,
				Query:         q.Query,
				ParameterOIDs: q.ParameterOIDs,
			})

			hash := rst.Client().PreparedStatementQueryHashByName(q.Name)

			spqrlog.Zero.Debug().
				Str("name", q.Name).
				Str("query", q.Query).
				Uint64("hash", hash).
				Uint("client", rst.Client().ID()).
				Msg("Parsing prepared statement")

			if config.RouterConfig().PgprotoDebug {
				if err := rst.Client().ReplyDebugNoticef("name %v, query %v, hash %d", q.Name, q.Query, hash); err != nil {
					return err
				}
			}

			fin, err := rst.PrepareRelayStepOnAnyRoute()
			if err != nil {
				return err
			}

			/* TODO: refactor code to make this less ugly */
			saveTxStatus := rst.qse.TxStatus()

			_, retMsg, err := rst.DeployPrepStmt(q.Name)
			if err != nil {
				return err
			}

			rst.qse.SetTxStatus(saveTxStatus)

			// tdb: fix this
			rst.plainQ = q.Query

			if err := rst.Client().Send(retMsg); err != nil {
				return err
			}

			if err := fin(); err != nil {
				return err
			}

		case *pgproto3.Bind:
			spqrlog.Zero.Debug().
				Str("name", q.PreparedStatement).
				Uint("client", rst.Client().ID()).
				Msg("Binding prepared statement")

			// Here we are going to actually redirect the query to the execution shard.
			// However, to execute commit, rollbacks, etc., we need to wait for the next query
			// or process it locally (set statement)

			def := rst.Client().PreparedStatementDefinitionByName(q.PreparedStatement)

			// We implicitly assume that there is always Execute after Bind for the same portal.
			// however, postgresql protocol allows some more cases.
			if err := rst.Client().ReplyBindComplete(); err != nil {
				return err
			}

			rst.execute = func() error {
				return nil
			}

			err := ProcQueryAdvancedTx(rst, def.Query, func() error {
				rst.saveBind = &pgproto3.Bind{}
				rst.saveBind.DestinationPortal = q.DestinationPortal

				rst.lastBindName = q.PreparedStatement
				hash := rst.Client().PreparedStatementQueryHashByName(q.PreparedStatement)

				rst.saveBind.PreparedStatement = fmt.Sprintf("%d", hash)
				rst.saveBind.ParameterFormatCodes = q.ParameterFormatCodes
				rst.Client().SetBindParams(q.Parameters)
				rst.Client().SetParamFormatCodes(q.ParameterFormatCodes)
				rst.saveBind.ResultFormatCodes = q.ResultFormatCodes
				rst.saveBind.Parameters = q.Parameters

				// Do not respond with BindComplete, as the relay step should take care of itself.
				_, err := rst.PrepareRelayStep()
				spqrlog.Zero.Debug().Uint("client", rst.Client().ID()).Interface("iface", rst.routingState).Err(err).Msg("executing proc query adv callback prepare relay step")

				if err != nil {
					return err
				}

				// hold route if appropriate

				if holdRoute {
					rst.HoldRouting()
				}

				switch rst.routingState.(type) {
				case plan.DDLState:
					routes := rst.Qr.DataShardsRoutes()
					if err := rst.procRoutes(routes); err != nil {
						return err
					}

					pstmt := rst.Client().PreparedStatementDefinitionByName(q.PreparedStatement)
					hash := rst.Client().PreparedStatementQueryHashByName(pstmt.Name)
					pstmt.Name = fmt.Sprintf("%d", hash)
					q.PreparedStatement = pstmt.Name
					err := rst.multishardPrepareDDL(hash, pstmt)
					if err != nil {
						return err
					}

					rst.execute = func() error {
						rst.AddQuery(msg)
						rst.AddQuery(&pgproto3.Execute{})
						rst.AddQuery(&pgproto3.Sync{})

						_, err := rst.RelayFlush(true, true)
						// do not complete relay here yet
						return err
					}

					return nil
				case plan.VirtualPlan:
					rst.execute = func() error {
						rst.AddQuery(msg)

						rst.AddQuery(&pgproto3.Execute{})

						rst.AddQuery(&pgproto3.Sync{})
						// do not complete relay here yet
						_, err = rst.RelayFlush(true, true)
						return err
					}
					return nil
				}

				// TODO: multi-shard statements
				if rst.bindRoute == nil {
					routes := rst.CurrentRoutes()
					if len(routes) == 1 {
						rst.bindRoute = &routes[0]
					} else {
						err := fmt.Errorf("failed to deploy prepared statement")

						spqrlog.Zero.Error().Uint("client", rst.Client().ID()).Err(err).Msg("query adv callback")

						return err
					}
				}

				rst.execute = func() error {
					err := rst.PrepareRelayStepOnHintRoute(rst.bindRoute)
					if err != nil {
						return err
					}

					_, _, err = rst.DeployPrepStmt(q.PreparedStatement)
					if err != nil {
						return err
					}

					/* Case when no describe stmt was issued before Execute+Sync*/
					if rst.saveBind != nil {
						rst.AddSilentQuery(rst.saveBind)
						// do not send saved bind twice
					}

					rst.AddQuery(&pgproto3.Execute{})

					rst.AddQuery(&pgproto3.Sync{})
					// do not complete relay here yet
					_, err = rst.RelayFlush(true, true)
					return err
				}

				return nil
			}, true /* cache parsing for prep statement */, false /* do not completeRelay*/)

			if err != nil {
				return err
			}

		case *pgproto3.Describe:
			// save txstatus because it may be overwritten if we have no backend connection
			saveTxStat := rst.qse.TxStatus()

			if q.ObjectType == 'P' {
				spqrlog.Zero.Debug().
					Uint("client", rst.Client().ID()).
					Str("last-bind-name", rst.lastBindName).
					Msg("Describe portal")

				if cachedPd, ok := rst.savedPortalDesc[rst.lastBindName]; ok {
					if cachedPd.rd != nil {
						// send to the client
						if err := rst.Client().Send(cachedPd.rd); err != nil {
							return err
						}
					}
					if cachedPd.nodata != nil {
						// send to the client
						if err := rst.Client().Send(cachedPd.nodata); err != nil {
							return err
						}
					}
				} else {
					cachedPd = PortalDesc{}

					err := rst.PrepareRelayStepOnHintRoute(rst.bindRoute)
					if err != nil {
						return err
					}

					switch q := rst.routingState.(type) {
					case plan.DDLState:
						pstmt := rst.Client().PreparedStatementDefinitionByName(rst.lastBindName)
						hash := rst.Client().PreparedStatementQueryHashByName(pstmt.Name)
						pstmt.Name = fmt.Sprintf("%d", hash)

						pd, err := rst.multishardDescribePortal(rst.saveBind)
						if err != nil {
							return err
						}
						if pd.RowDesc != nil {
							// send to the client
							if err := rst.Client().Send(pd.RowDesc); err != nil {
								return err
							}
						}
						if pd.NoData {
							// send to the client
							if err := rst.Client().Send(&pgproto3.NoData{}); err != nil {
								return err
							}
						}
						break singleMsgLoop
					case plan.VirtualPlan:
						// skip deploy

						cachedPd.rd = &pgproto3.RowDescription{
							Fields: q.VirtualRowCols,
						}

						// send to the client
						if err := rst.Client().Send(cachedPd.rd); err != nil {
							return err
						}

						cachedPd.nodata = nil

						rst.savedPortalDesc[rst.lastBindName] = cachedPd
						break singleMsgLoop
					default:
						if _, _, err := rst.DeployPrepStmt(rst.lastBindName); err != nil {
							return err
						}
					}

					// do not send saved bind twice
					if rst.saveBind == nil {
						// wtf?
						return fmt.Errorf("failed to describe statement, stmt was never deployed")
					}

					_, err = rst.RelayStep(rst.saveBind, false, false)
					if err != nil {
						return err
					}

					_, err = rst.RelayStep(q, false, false)
					if err != nil {
						return err
					}

					/* Here we close portal, so other clients can reuse it */
					_, err = rst.RelayStep(&pgproto3.Close{
						ObjectType: 'P',
					}, false, false)
					if err != nil {
						return err
					}

					unreplied, err := rst.RelayStep(&pgproto3.Sync{}, true, false)
					if err != nil {
						return err
					}

					for _, msg := range unreplied {
						spqrlog.Zero.Debug().Type("msg type", msg).Msg("describe portal unreplied message")
						// https://www.postgresql.org/docs/current/protocol-flow.html
						switch qq := msg.(type) {
						case *pgproto3.RowDescription:

							cachedPd.rd = &pgproto3.RowDescription{}

							cachedPd.rd.Fields = make([]pgproto3.FieldDescription, len(qq.Fields))

							for i := range len(qq.Fields) {
								s := make([]byte, len(qq.Fields[i].Name))
								copy(s, qq.Fields[i].Name)

								cachedPd.rd.Fields[i] = qq.Fields[i]
								cachedPd.rd.Fields[i].Name = s
							}
							// send to the client
							if err := rst.Client().Send(qq); err != nil {
								return err
							}
						case *pgproto3.NoData:
							cpQ := *qq
							cachedPd.nodata = &cpQ
							// send to the client
							if err := rst.Client().Send(qq); err != nil {
								return err
							}
						default:
							// error out? panic? protoc violation?
							// no, just chill
						}
					}

					rst.savedPortalDesc[rst.lastBindName] = cachedPd
				}
			} else {
				/* q.ObjectType == 'S' */
				spqrlog.Zero.Debug().
					Uint("client", rst.Client().ID()).
					Str("stmt-name", q.Name).
					Msg("Describe prep statement")

				fin, err := rst.PrepareRelayStepOnAnyRoute()
				if err != nil {
					return err
				}

				rd, _, err := rst.DeployPrepStmt(q.Name)
				if err != nil {
					return err
				}

				if rd.ParamDesc != nil {
					if err := rst.Client().Send(rd.ParamDesc); err != nil {
						return err
					}
				}

				if rd.NoData {
					if err := rst.Client().Send(&pgproto3.NoData{}); err != nil {
						return err
					}
				} else {
					if rd.RowDesc != nil {
						if err := rst.Client().Send(rd.RowDesc); err != nil {
							return err
						}
					}
				}

				if err := fin(); err != nil {
					return err
				}
			}

			rst.qse.SetTxStatus(saveTxStat)

		case *pgproto3.Execute:
			spqrlog.Zero.Debug().
				Uint("client", rst.Client().ID()).
				Msg("Execute prepared statement, reset saved bind")
			err := rst.execute()
			rst.execute = nil
			rst.bindRoute = nil
			if err != nil {
				return err
			}
		case *pgproto3.Close:
			//
		default:
			panic(fmt.Sprintf("unexpected query type %v", msg))
		}
	}

	statistics.RecordStartTime(statistics.Shard, time.Now(), rst.Client().ID())
	return rst.CompleteRelay(true)
}

// TODO : unit tests
func (rst *RelayStateImpl) Parse(query string, doCaching bool) (parser.ParseState, string, error) {
	if cache, ok := rst.parseCache[query]; ok {
		rst.qp.SetStmt(cache.stmt)
		return cache.ps, cache.comm, nil
	}

	state, comm, err := rst.qp.Parse(query)

	switch stm := rst.qp.Stmt().(type) {
	case *lyx.Insert:
		// load columns from information schema
		// Do not check err here, just keep going
		if len(stm.Columns) == 0 {
			switch tableref := stm.TableRef.(type) {
			case *lyx.RangeVar:
				cptr := rst.Qr.SchemaCache()
				if cptr != nil {
					var schemaErr error
					stm.Columns, schemaErr = cptr.GetColumns(rst.Cl.DB(), tableref.SchemaName, tableref.RelationName)
					if schemaErr != nil {
						spqrlog.Zero.Err(schemaErr).Msg("get columns from schema cache")
					}
				}
			}
		}
	}

	if err == nil && doCaching {
		stmt := rst.qp.Stmt()
		/* only cache specific type of queries */
		switch stmt.(type) {
		case *lyx.Select, *lyx.Insert, *lyx.Update, *lyx.Delete:
			rst.parseCache[query] = ParseCacheEntry{
				ps:   state,
				comm: comm,
				stmt: stmt,
			}
		}
	}

	rst.plainQ = query
	return state, comm, err
}

var _ RelayStateMgr = &RelayStateImpl{}

// TODO : unit tests
func (rst *RelayStateImpl) PrepareRelayStep() (plan.Plan, error) {
	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Str("user", rst.Client().Usr()).
		Str("db", rst.Client().DB()).
		Msg("preparing relay step for client")

	if rst.holdRouting {
		return nil, nil
	}

	// txactive == 0 || activeSh == nil
	if !rst.poolMgr.ValidateReRoute(rst) {
		if rst.Client().EnhancedMultiShardProcessing() {
			/* With engine v2 we can expand transaction on more targets */
			/* TODO: XXX */

			r, q, err := rst.Reroute()
			if err != nil {
				return nil, err
			}

			/*
			 * Try to keep single-shard connection as long as possible
			 */
			if len(r) == 1 && len(rst.ActiveShards()) == 1 && rst.ActiveShards()[0].Name == r[0].Name {
				return q, nil
			}

			/* else expand transaction */
			return q, rst.expandRoutes(r)
		}
		return nil, nil
	}

	r, q, err := rst.Reroute()

	switch err {
	case nil:
		switch q.(type) {
		case plan.VirtualPlan:
			return q, nil
		default:
			return q, rst.procRoutes(r)
		}
	case ErrSkipQuery:
		if err := rst.Client().ReplyErr(err); err != nil {
			return nil, err
		}
		return nil, ErrSkipQuery
	case ErrMatchShardError:
		_ = rst.Client().ReplyErrMsgByCode(spqrerror.SPQR_NO_DATASHARD)
		return nil, ErrSkipQuery
	default:
		rst.msgBuf = nil
		return q, err
	}
}

var noopCloseRouteFunc = func() error {
	return nil
}

// TODO : unit tests
func (rst *RelayStateImpl) PrepareRelayStepOnHintRoute(route *kr.ShardKey) error {
	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Str("user", rst.Client().Usr()).
		Str("db", rst.Client().DB()).
		Int("curr routes len", len(rst.activeShards)).
		Interface("route", route).
		Msg("preparing relay step for client on target route")

	if rst.holdRouting {
		return nil
	}

	// txactive == 0 || activeSh == nil
	// already has route, no need for any hint
	if !rst.poolMgr.ValidateReRoute(rst) {
		return nil
	}

	if route == nil {
		return fmt.Errorf("failed to use hint route")
	}

	switch err := rst.RerouteToTargetRoute(route); err {
	case nil:
		return nil
	case ErrSkipQuery:
		if err := rst.Client().ReplyErr(err); err != nil {
			return err
		}
		return ErrSkipQuery
	case ErrMatchShardError:
		_ = rst.Client().ReplyErrMsgByCode(spqrerror.SPQR_NO_DATASHARD)
		return ErrSkipQuery
	default:
		rst.msgBuf = nil
		return err
	}
}

// TODO : unit tests
func (rst *RelayStateImpl) PrepareRelayStepOnAnyRoute() (func() error, error) {
	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Str("user", rst.Client().Usr()).
		Str("db", rst.Client().DB()).
		Msg("preparing relay step for client on any route")

	if rst.holdRouting {
		return noopCloseRouteFunc, nil
	}

	// txactive == 0 || activeSh == nil
	if !rst.poolMgr.ValidateReRoute(rst) {
		return noopCloseRouteFunc, nil
	}

	switch err := rst.RerouteToRandomRoute(); err {
	case nil:
		routes := rst.CurrentRoutes()
		return func() error {
			return rst.Unroute(routes)
		}, nil
	case ErrSkipQuery:
		if err := rst.Client().ReplyErr(err); err != nil {
			return noopCloseRouteFunc, err
		}
		return noopCloseRouteFunc, ErrSkipQuery
	case ErrMatchShardError:
		_ = rst.Client().ReplyErrMsgByCode(spqrerror.SPQR_NO_DATASHARD)
		return noopCloseRouteFunc, ErrSkipQuery
	default:
		rst.msgBuf = nil
		return noopCloseRouteFunc, err
	}
}

// TODO : unit tests
func (rst *RelayStateImpl) ProcessMessageBuf(waitForResp, replyCl bool) error {
	if _, err := rst.PrepareRelayStep(); err != nil {
		return err
	}

	statistics.RecordStartTime(statistics.Shard, time.Now(), rst.Client().ID())

	_, err := rst.RelayFlush(waitForResp, replyCl)
	return err
}

// TODO : unit tests
func (rst *RelayStateImpl) ProcessMessage(
	msg pgproto3.FrontendMessage,
	waitForResp, replyCl bool) error {
	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Msg("relay step: process message for client")
	if _, err := rst.PrepareRelayStep(); err != nil {
		return err
	}

	statistics.RecordStartTime(statistics.Shard, time.Now(), rst.Client().ID())

	if _, err := rst.RelayStep(msg, waitForResp, replyCl); err != nil {
		if err := rst.CompleteRelay(replyCl); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("failed to complete relay")
			return err
		}
		return err
	}

	return rst.CompleteRelay(replyCl)
}
