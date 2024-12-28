package relay

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/hashfunction"
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
	"github.com/pg-sharding/spqr/router/pgcopy"
	"github.com/pg-sharding/spqr/router/poolmgr"
	"github.com/pg-sharding/spqr/router/qrouter"
	"github.com/pg-sharding/spqr/router/route"
	"github.com/pg-sharding/spqr/router/routingstate"
	"github.com/pg-sharding/spqr/router/server"
	"github.com/pg-sharding/spqr/router/statistics"
	"golang.org/x/exp/slices"
)

type RelayStateMgr interface {
	poolmgr.ConnectionKeeper
	route.RouteMgr
	QueryRouter() qrouter.QueryRouter
	Reset() error
	StartTrace()
	Flush()

	ConnMgr() poolmgr.PoolMgr

	ShouldRetry(err error) bool
	Parse(query string, doCaching bool) (parser.ParseState, string, error)

	AddQuery(q pgproto3.FrontendMessage)
	AddSilentQuery(q pgproto3.FrontendMessage)
	TxActive() bool

	PgprotoDebug() bool

	RelayStep(msg pgproto3.FrontendMessage, waitForResp bool, replyCl bool) (txstatus.TXStatus, []pgproto3.BackendMessage, error)

	CompleteRelay(replyCl bool) error
	Close() error
	Client() client.RouterClient

	ProcessMessage(msg pgproto3.FrontendMessage, waitForResp, replyCl bool, cmngr poolmgr.PoolMgr) error
	PrepareStatement(hash uint64, d *prepstatement.PreparedStatementDefinition) (*prepstatement.PreparedStatementDescriptor, pgproto3.BackendMessage, error)

	PrepareRelayStep(cmngr poolmgr.PoolMgr) error
	PrepareRelayStepOnAnyRoute(cmngr poolmgr.PoolMgr) (func() error, error)
	PrepareRelayStepOnHintRoute(cmngr poolmgr.PoolMgr, route *routingstate.DataShardRoute) error

	HoldRouting()
	UnholdRouting()

	ProcessMessageBuf(waitForResp, replyCl, completeRelay bool, cmngr poolmgr.PoolMgr) (bool, error)
	RelayRunCommand(msg pgproto3.FrontendMessage, waitForResp bool, replyCl bool) error

	ProcQuery(query pgproto3.FrontendMessage, waitForResp bool, replyCl bool) (txstatus.TXStatus, []pgproto3.BackendMessage, bool, error)
	ProcCopyPrepare(ctx context.Context, stmt *lyx.Copy) (*pgcopy.CopyState, error)
	ProcCopy(ctx context.Context, data *pgproto3.CopyData, copyState *pgcopy.CopyState) ([]byte, error)

	ProcCommand(query pgproto3.FrontendMessage, waitForResp bool, replyCl bool) error

	ProcCopyComplete(query *pgproto3.FrontendMessage) error

	RouterMode() config.RouterMode

	AddExtendedProtocMessage(q pgproto3.FrontendMessage)
	ProcessExtendedBuffer(cmngr poolmgr.PoolMgr) error
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
	txStatus   txstatus.TXStatus
	CopyActive bool

	activeShards   []kr.ShardKey
	TargetKeyRange kr.KeyRange

	traceMsgs          bool
	WorldShardFallback bool
	routerMode         config.RouterMode

	pgprotoDebug bool

	routingState routingstate.RoutingState

	Qr      qrouter.QueryRouter
	qp      parser.QParser
	plainQ  string
	Cl      client.RouterClient
	manager poolmgr.PoolMgr

	maintain_params bool

	msgBuf []BufferedMessage

	holdRouting bool

	bindRoute    *routingstate.DataShardRoute
	lastBindName string

	execute func() error

	saveBind        *pgproto3.Bind
	savedPortalDesc map[string]PortalDesc

	parseCache map[string]ParseCacheEntry

	// buffer of messages to process on Sync request
	xBuf []pgproto3.FrontendMessage
}

// HoldRouting implements RelayStateMgr.
func (rst *RelayStateImpl) HoldRouting() {
	rst.holdRouting = true
}

// UnholdRouting implements RelayStateMgr.
func (rst *RelayStateImpl) UnholdRouting() {
	rst.holdRouting = false
}

func NewRelayState(qr qrouter.QueryRouter, client client.RouterClient, manager poolmgr.PoolMgr, rcfg *config.Router) RelayStateMgr {
	return &RelayStateImpl{
		activeShards:       nil,
		txStatus:           txstatus.TXIDLE,
		msgBuf:             nil,
		traceMsgs:          false,
		Qr:                 qr,
		Cl:                 client,
		manager:            manager,
		WorldShardFallback: rcfg.WorldShardFallback,
		routerMode:         config.RouterMode(rcfg.RouterMode),
		maintain_params:    rcfg.MaintainParams,
		pgprotoDebug:       rcfg.PgprotoDebug,
		execute:            nil,
		savedPortalDesc:    map[string]PortalDesc{},
		parseCache:         map[string]ParseCacheEntry{},
	}
}

func (rst *RelayStateImpl) SyncCount() int64 {
	if rst.Cl.Server() == nil {
		return 0
	}
	return rst.Cl.Server().Sync()
}

func (rst *RelayStateImpl) DataPending() bool {
	if rst.Cl.Server() == nil {
		return false
	}

	return rst.Cl.Server().DataPending()
}

func (rst *RelayStateImpl) QueryRouter() qrouter.QueryRouter {
	return rst.Qr
}

func (rst *RelayStateImpl) SetTxStatus(status txstatus.TXStatus) {
	rst.txStatus = status
}

func (rst *RelayStateImpl) PgprotoDebug() bool {
	return rst.pgprotoDebug
}

func (rst *RelayStateImpl) Client() client.RouterClient {
	return rst.Cl
}

func (rst *RelayStateImpl) TxStatus() txstatus.TXStatus {
	return rst.txStatus
}

// TODO : unit tests
func (rst *RelayStateImpl) PrepareStatement(hash uint64, d *prepstatement.PreparedStatementDefinition) (*prepstatement.PreparedStatementDescriptor, pgproto3.BackendMessage, error) {
	serv := rst.Client().Server()

	if ok, rd := serv.HasPrepareStatement(hash); ok {
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

	_, unreplied, err := rst.RelayStep(&pgproto3.Sync{}, true, false)
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
		// dont need to complete relay because tx state didt changed
		rst.Cl.Server().StorePrepareStatement(hash, d, rd)
	}
	return rd, retMsg, nil
}

func (rst *RelayStateImpl) RouterMode() config.RouterMode {
	return rst.routerMode
}

func (rst *RelayStateImpl) Close() error {
	defer rst.Cl.Close()
	defer rst.ActiveShardsReset()
	return rst.manager.UnRouteCB(rst.Cl, rst.activeShards)
}

func (rst *RelayStateImpl) TxActive() bool {
	return rst.txStatus == txstatus.TXACT
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
	rst.txStatus = txstatus.TXIDLE

	_ = rst.Cl.Reset()

	return rst.Cl.Unroute()
}

// TODO : unit tests
func (rst *RelayStateImpl) StartTrace() {
	rst.traceMsgs = true
}

// TODO : unit tests
func (rst *RelayStateImpl) Flush() {
	rst.msgBuf = nil
	rst.traceMsgs = false
}

var ErrSkipQuery = fmt.Errorf("wait for a next query")

// TODO : unit tests
func (rst *RelayStateImpl) procRoutes(routes []*routingstate.DataShardRoute) error {
	// if there is no routes configurted, there is nowhere to route to
	if len(routes) == 0 {
		return qrouter.MatchShardError
	}

	spqrlog.Zero.Debug().
		Uint("relay state", spqrlog.GetPointer(rst)).
		Msg("unroute previous connections")

	if err := rst.Unroute(rst.activeShards); err != nil {
		return err
	}

	rst.activeShards = nil
	for _, shr := range routes {
		rst.activeShards = append(rst.activeShards, shr.Shkey)
	}

	if rst.PgprotoDebug() {
		if err := rst.Cl.ReplyDebugNoticef("matched datashard routes %+v", routes); err != nil {
			return err
		}
	}

	if err := rst.Connect(routes); err != nil {
		spqrlog.Zero.Error().
			Err(err).
			Uint("client", rst.Client().ID()).
			Msg("client encounter while initialing server connection")

		_ = rst.Reset()
		return err
	}

	return nil
}

// TODO : unit tests
func (rst *RelayStateImpl) Reroute() error {
	_ = rst.Cl.ReplyDebugNotice("rerouting the client connection")

	span := opentracing.StartSpan("reroute")
	defer span.Finish()
	span.SetTag("user", rst.Cl.Usr())
	span.SetTag("db", rst.Cl.DB())

	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Interface("params", rst.Client().BindParams()).
		Msg("rerouting the client connection, resolving shard")

	var routingState routingstate.RoutingState
	var err error

	if v := rst.Client().ExecuteOn(); v != "" {
		routingState = routingstate.ShardMatchState{
			Route: &routingstate.DataShardRoute{
				Shkey: kr.ShardKey{
					Name: v,
				},
			},
		}
	} else {
		routingState, err = rst.Qr.Route(context.TODO(), rst.qp.Stmt(), rst.Cl)
	}

	if err != nil {
		return fmt.Errorf("error processing query '%v': %v", rst.plainQ, err)
	}
	rst.routingState = routingState
	switch v := routingState.(type) {
	case routingstate.MultiMatchState:
		if rst.TxActive() {
			return fmt.Errorf("cannot route in an active transaction")
		}
		spqrlog.Zero.Debug().
			Uint("client", rst.Client().ID()).
			Err(err).
			Msgf("parsed MultiMatchState")
		return rst.procRoutes(rst.Qr.DataShardsRoutes())
	case routingstate.DDLState:
		spqrlog.Zero.Debug().
			Uint("client", rst.Client().ID()).
			Err(err).
			Msgf("parsed DDLState")
		return rst.procRoutes(rst.Qr.DataShardsRoutes())
	case routingstate.ShardMatchState:
		// TBD: do it better
		return rst.procRoutes([]*routingstate.DataShardRoute{v.Route})
	case routingstate.SkipRoutingState:
		return ErrSkipQuery
	case routingstate.RandomMatchState:
		return rst.RerouteToRandomRoute()
	case routingstate.WorldRouteState:
		if !rst.WorldShardFallback {
			return err
		}

		// fallback to execute query on world datashard (s)
		_, _ = rst.RerouteWorld()
		if err := rst.ConnectWorld(); err != nil {
			return err
		}

		return nil
	default:
		return fmt.Errorf("unexpected route state %T", v)
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

	routes := rst.Qr.DataShardsRoutes()
	if len(routes) == 0 {
		return fmt.Errorf("no routes configured")
	}

	routingState := routingstate.ShardMatchState{
		Route: routes[rand.Int()%len(routes)],
	}
	rst.routingState = routingState

	return rst.procRoutes([]*routingstate.DataShardRoute{routingState.Route})
}

// TODO : unit tests
func (rst *RelayStateImpl) RerouteToTargetRoute(route *routingstate.DataShardRoute) error {
	_ = rst.Cl.ReplyDebugNotice("rerouting the client connection")

	span := opentracing.StartSpan("reroute")
	defer span.Finish()
	span.SetTag("user", rst.Cl.Usr())
	span.SetTag("db", rst.Cl.DB())

	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Interface("statement", rst.qp.Stmt()).
		Msg("rerouting the client connection to target shard, resolving shard")

	routingState := routingstate.ShardMatchState{
		Route: route,
	}
	rst.routingState = routingState

	return rst.procRoutes([]*routingstate.DataShardRoute{route})
}

// TODO : unit tests
func (rst *RelayStateImpl) CurrentRoutes() []*routingstate.DataShardRoute {
	switch q := rst.routingState.(type) {
	case routingstate.ShardMatchState:
		return []*routingstate.DataShardRoute{q.Route}
	default:
		return nil
	}
}

func (rst *RelayStateImpl) RerouteWorld() ([]*routingstate.DataShardRoute, error) {
	span := opentracing.StartSpan("reroute to world")
	defer span.Finish()
	span.SetTag("user", rst.Cl.Usr())
	span.SetTag("db", rst.Cl.DB())

	shardRoutes := rst.Qr.WorldShardsRoutes()

	if len(shardRoutes) == 0 {
		_ = rst.manager.UnRouteWithError(rst.Cl, nil, qrouter.MatchShardError)
		return nil, qrouter.MatchShardError
	}

	if err := rst.Unroute(rst.activeShards); err != nil {
		return nil, err
	}

	for _, shr := range shardRoutes {
		rst.activeShards = append(rst.activeShards, shr.Shkey)
	}

	return shardRoutes, nil
}

// TODO : unit tests
func (rst *RelayStateImpl) Connect(shardRoutes []*routingstate.DataShardRoute) error {
	var serv server.Server
	var err error

	if len(shardRoutes) > 1 {
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

	if err := rst.manager.RouteCB(rst.Cl, rst.activeShards); err != nil {
		return err
	}

	/* take care of session param if we told to */
	if rst.Client().MaintainParams() {
		query := rst.Cl.ConstructClientParams()
		spqrlog.Zero.Debug().
			Uint("client", rst.Client().ID()).
			Str("query", query.String).
			Msg("setting params for client")
		_, _, _, err = rst.ProcQuery(query, true, false)
	}
	return err
}

func (rst *RelayStateImpl) ConnectWorld() error {
	_ = rst.Cl.ReplyDebugNotice("open a connection to the single data shard")

	serv := server.NewShardServer(rst.Cl.Route().ServPool())
	if err := rst.Cl.AssignServerConn(serv); err != nil {
		return err
	}

	spqrlog.Zero.Debug().
		Str("user", rst.Cl.Usr()).
		Str("db", rst.Cl.DB()).
		Msg("route client to world datashard")

	return rst.manager.RouteCB(rst.Cl, rst.activeShards)
}

// TODO : unit tests
func (rst *RelayStateImpl) ProcCommand(query pgproto3.FrontendMessage, waitForResp bool, replyCl bool) error {

	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Interface("query", query).
		Msg("client process command")
	_ = rst.
		Client().
		ReplyDebugNotice(
			fmt.Sprintf("executing your query %v", query)) // TODO performance issue

	if err := rst.Client().Server().Send(query); err != nil {
		return err
	}

	if !waitForResp {
		return nil
	}

	for {
		msg, err := rst.Client().Server().Receive()
		if err != nil {
			return err
		}

		switch v := msg.(type) {
		case *pgproto3.CommandComplete:
			return nil
		case *pgproto3.ErrorResponse:
			return fmt.Errorf("%s", v.Message)
		default:
			spqrlog.Zero.Debug().
				Uint("client", rst.Client().ID()).
				Type("message-type", v).
				Msg("got message from server")
			if replyCl {
				err = rst.Client().Send(msg)
				if err != nil {
					return err
				}
			}
		}
	}
}

// TODO : unit tests
func (rst *RelayStateImpl) RelayCommand(v pgproto3.FrontendMessage, waitForResp bool, replyCl bool) error {
	if !rst.TxActive() {
		if err := rst.manager.TXBeginCB(rst); err != nil {
			return err
		}
		rst.txStatus = txstatus.TXACT
	}

	return rst.ProcCommand(v, waitForResp, replyCl)
}

func (rst *RelayStateImpl) RelayRunCommand(msg pgproto3.FrontendMessage, waitForResp bool, replyCl bool) error {
	return rst.ProcCommand(msg, waitForResp, replyCl)
}

// TODO: unit tests
func (rst *RelayStateImpl) ProcCopyPrepare(ctx context.Context, stmt *lyx.Copy) (*pgcopy.CopyState, error) {
	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Msg("client pre-process copy")

	var relname string

	switch q := stmt.TableRef.(type) {
	case *lyx.RangeVar:
		relname = q.RelationName
	}

	// TODO: check by whole RFQN
	ds, err := rst.Qr.Mgr().GetRelationDistribution(ctx, relname)
	if err != nil {
		return nil, err
	}
	if ds.Id == distributions.REPLICATED {
		return &pgcopy.CopyState{
			Scatter: true,
		}, nil
	}

	// Read delimiter from COPY options
	delimiter := byte('\t')
	allow_multishard := rst.Client().AllowMultishard()
	for _, opt := range stmt.Options {
		o := opt.(*lyx.Option)
		if strings.ToLower(o.Name) == "delimiter" {
			delimiter = o.Arg.(*lyx.AExprSConst).Value[0]
		}
		if strings.ToLower(o.Name) == "format" {
			if o.Arg.(*lyx.AExprSConst).Value == "csv" {
				delimiter = ','
			}
		}
	}

	if rst.Client().ExecuteOn() != "" {
		return &pgcopy.CopyState{
			Delimiter:       delimiter,
			ExpRoute:        &routingstate.DataShardRoute{},
			AllowMultishard: allow_multishard,
		}, nil
	}

	TargetType := ds.ColTypes[0]

	if len(ds.ColTypes) != 1 {
		return nil, fmt.Errorf("multi-column copy processing is not yet supported")
	}

	var hashFunc hashfunction.HashFunctionType

	if v, err := hashfunction.HashFunctionByName(ds.Relations[relname].DistributionKey[0].HashFunction); err != nil {
		return nil, err
	} else {
		hashFunc = v
	}

	krs, err := rst.Qr.Mgr().ListKeyRanges(ctx, ds.Id)
	if err != nil {
		return nil, err
	}

	colOffset := -1
	for indx, c := range stmt.Columns {
		if c == ds.Relations[relname].DistributionKey[0].Column {
			colOffset = indx
			break
		}
	}
	if colOffset == -1 {
		return nil, fmt.Errorf("failed to resolve target copy column offset")
	}

	return &pgcopy.CopyState{
		Delimiter:       delimiter,
		ExpRoute:        &routingstate.DataShardRoute{},
		AllowMultishard: allow_multishard,
		Krs:             krs,
		TargetType:      TargetType,
		HashFunc:        hashFunc,
	}, nil
}

// TODO : unit tests
func (rst *RelayStateImpl) ProcCopy(ctx context.Context, data *pgproto3.CopyData, cps *pgcopy.CopyState) ([]byte, error) {
	if v := rst.Client().ExecuteOn(); v != "" {
		for _, sh := range rst.Client().Server().Datashards() {
			if sh.Name() == v {
				err := sh.Send(data)
				return nil, err
			}
		}
		return nil, fmt.Errorf("metadata corrutped")
	}

	/* We dont really need to parse and route tuples for DISTRIBUTED relations */
	if cps.Scatter {
		return nil, rst.Client().Server().Send(data)
	}

	var leftOvermsgData []byte = nil

	rowsMp := map[string][]byte{}

	values := make([]interface{}, 0)

	// Parse data
	// and decide where to route
	prevDelimiter := 0
	prevLine := 0
	currentAttr := 0

	for i, b := range data.Data {
		if i+2 < len(data.Data) && string(data.Data[i:i+2]) == "\\." {
			prevLine = len(data.Data)
			break
		}
		if b == '\n' || b == cps.Delimiter {

			if currentAttr == cps.ColumnOffset {
				tmp, err := hashfunction.ApplyHashFunctionOnStringRepr(data.Data[prevDelimiter:i], cps.TargetType, cps.HashFunc)
				if err != nil {
					return nil, err
				}
				values = append(values, tmp)
			}

			currentAttr++
			prevDelimiter = i + 1
		}
		if b != '\n' {
			continue
		}

		// check where this tuple should go
		currroute, err := rst.Qr.DeparseKeyWithRangesInternal(ctx, values, cps.Krs)
		if err != nil {
			return nil, err
		}

		if currroute == nil {
			return nil, fmt.Errorf("multishard copy is not supported: %+v at line number %d %d %v", values[0], prevLine, i, b)
		}

		if !cps.AllowMultishard {
			if cps.ExpRoute.Shkey.Name == "" {
				*cps.ExpRoute = *currroute
			}
		}

		if !cps.AllowMultishard && currroute.Shkey.Name != cps.ExpRoute.Shkey.Name {
			return nil, fmt.Errorf("multishard copy is not supported")
		}

		values = nil
		rowsMp[currroute.Shkey.Name] = append(rowsMp[currroute.Shkey.Name], data.Data[prevLine:i+1]...)
		currentAttr = 0
		prevLine = i + 1
	}

	if prevLine != len(data.Data) {
		if spqrlog.IsDebugLevel() {
			_ = rst.Client().ReplyNotice(fmt.Sprintf("leftover data saved to next iter %d - %d", prevLine, len(data.Data)))
		}
		leftOvermsgData = data.Data[prevLine:len(data.Data)]
	}

	for _, sh := range rst.Client().Server().Datashards() {
		if !cps.AllowMultishard {
			if cps.ExpRoute != nil && sh.Name() == cps.ExpRoute.Shkey.Name {
				err := sh.Send(&pgproto3.CopyData{Data: rowsMp[sh.Name()]})
				return leftOvermsgData, err
			}
		} else {
			bts, ok := rowsMp[sh.Name()]
			if ok {
				err := sh.Send(&pgproto3.CopyData{Data: bts})
				if err != nil {
					return nil, err
				}
			}
		}
	}

	// shouldn't exit from here
	return leftOvermsgData, nil
}

// TODO : unit tests
func (rst *RelayStateImpl) ProcCopyComplete(query *pgproto3.FrontendMessage) error {
	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Type("query-type", query).
		Msg("client process copy end")

	if err := rst.Client().Server().Send(*query); err != nil {
		return err
	}

	for {
		msg, err := rst.Client().Server().Receive()
		if err != nil {
			return err
		}
		switch msg.(type) {
		case *pgproto3.CommandComplete, *pgproto3.ErrorResponse:
			return rst.Client().Send(msg)
		default:
			if err := rst.Client().Send(msg); err != nil {
				return err
			}
		}
	}
}

// TODO : unit tests
func (rst *RelayStateImpl) ProcQuery(query pgproto3.FrontendMessage, waitForResp bool, replyCl bool) (txstatus.TXStatus, []pgproto3.BackendMessage, bool, error) {
	server := rst.Client().Server()

	if server == nil {
		return txstatus.TXERR, nil, false, fmt.Errorf("client %p is out of transaction sync with router", rst.Client())
	}

	spqrlog.Zero.Debug().
		Uints("shards", shard.ShardIDs(server.Datashards())).
		Type("query-type", query).
		Msg("relay process query")

	if err := server.Send(query); err != nil {
		return txstatus.TXERR, nil, false, err
	}

	waitForRespLocal := waitForResp

	switch query.(type) {
	case *pgproto3.Query:
		// ok
	case *pgproto3.Sync:
		// ok
	default:
		waitForRespLocal = false
	}

	if !waitForRespLocal {
		return txstatus.TXCONT, nil, true, nil
	}

	ok := true

	unreplied := make([]pgproto3.BackendMessage, 0)

	for {
		msg, err := server.Receive()
		if err != nil {
			return txstatus.TXERR, nil, false, err
		}

		switch v := msg.(type) {
		case *pgproto3.CopyInResponse:
			// handle replyCl somehow
			err = rst.Client().Send(msg)
			if err != nil {
				return txstatus.TXERR, nil, false, err
			}

			q := rst.qp.Stmt().(*lyx.Copy)

			if err := func() error {
				var leftOvermsgData []byte
				ctx := context.TODO()

				cps, err := rst.ProcCopyPrepare(ctx, q)
				if err != nil {
					return err
				}

				for {
					cpMsg, err := rst.Client().Receive()
					if err != nil {
						return err
					}

					switch newMsg := cpMsg.(type) {
					case *pgproto3.CopyData:
						leftOvermsgData = append(leftOvermsgData, newMsg.Data...)

						if leftOvermsgData, err = rst.ProcCopy(ctx, &pgproto3.CopyData{Data: leftOvermsgData}, cps); err != nil {
							return err
						}
					case *pgproto3.CopyDone, *pgproto3.CopyFail:
						if err := rst.ProcCopyComplete(&cpMsg); err != nil {
							return err
						}
						return nil
					default:
						/* panic? */
					}
				}
			}(); err != nil {
				return txstatus.TXERR, nil, false, err
			}
		case *pgproto3.ReadyForQuery:
			return txstatus.TXStatus(v.TxStatus), unreplied, ok, nil
		case *pgproto3.ErrorResponse:
			if replyCl {
				err = rst.Client().Send(msg)
				if err != nil {
					return txstatus.TXERR, nil, false, err
				}
			} else {
				unreplied = append(unreplied, msg)
			}
			ok = false
		// never resend this msgs
		case *pgproto3.ParseComplete:
			unreplied = append(unreplied, msg)
		case *pgproto3.BindComplete:
			unreplied = append(unreplied, msg)
		default:
			spqrlog.Zero.Debug().
				Str("server", server.Name()).
				Type("msg-type", v).
				Msg("received message from server")
			if replyCl {
				err = rst.Client().Send(msg)
				if err != nil {
					return txstatus.TXERR, nil, false, err
				}
			} else {
				unreplied = append(unreplied, msg)
			}
		}
	}
}

// TODO : unit tests
func (rst *RelayStateImpl) RelayFlush(waitForResp bool, replyCl bool) (txstatus.TXStatus, bool, error) {
	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Msg("flushing message buffer")

	ok := true

	flusher := func(buff []BufferedMessage, waitForResp, replyCl bool) (txstatus.TXStatus, error) {

		var txst txstatus.TXStatus
		var err error
		var txok bool

		for len(buff) > 0 {
			if !rst.TxActive() {
				if err := rst.manager.TXBeginCB(rst); err != nil {
					return 0, err
				}
			}

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

			if txst, _, txok, err = rst.ProcQuery(v.msg, waitForResp, resolvedReplyCl); err != nil {
				ok = false
				return txstatus.TXERR, err
			} else {
				ok = ok && txok
			}

			rst.SetTxStatus(txst)
		}

		return txst, nil
	}

	var txst txstatus.TXStatus
	var err error

	buf := rst.msgBuf
	rst.msgBuf = nil

	if txst, err = flusher(buf, waitForResp, replyCl); err != nil {
		return txst, false, err
	}

	return txst, ok, nil
}

// TODO : unit tests
func (rst *RelayStateImpl) RelayStep(msg pgproto3.FrontendMessage, waitForResp bool, replyCl bool) (txstatus.TXStatus, []pgproto3.BackendMessage, error) {
	if !rst.TxActive() {
		if err := rst.manager.TXBeginCB(rst); err != nil {
			return 0, nil, err
		}
	}

	bt, unreplied, _, err := rst.ProcQuery(msg, waitForResp, replyCl)
	if err != nil {
		rst.SetTxStatus(txstatus.TXERR)
		return txstatus.TXERR, unreplied, err
	}
	rst.SetTxStatus(bt)
	return rst.txStatus, unreplied, nil
}

func (rst *RelayStateImpl) ShouldRetry(err error) bool {
	return false
}

func (rst *RelayStateImpl) CompleteRelay(replyCl bool) error {
	if rst.CopyActive {
		return nil
	}
	statistics.RecordFinishedTransaction(time.Now(), rst.Client().ID())

	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Str("txstatus", rst.txStatus.String()).
		Msg("complete relay iter")

	switch rst.routingState.(type) {
	case routingstate.MultiMatchState:
		// TODO: explicitly forbid transaction, or hadnle it properly
		spqrlog.Zero.Debug().Msg("unroute multishard route")

		if err := rst.manager.TXEndCB(rst); err != nil {
			return nil
		}

		if replyCl {
			if err := rst.Cl.Send(&pgproto3.ReadyForQuery{
				TxStatus: byte(txstatus.TXIDLE),
			}); err != nil {
				return err
			}
		}

		return nil
	}

	switch rst.txStatus {
	case txstatus.TXIDLE:
		if replyCl {
			if err := rst.Cl.Send(&pgproto3.ReadyForQuery{
				TxStatus: byte(rst.txStatus),
			}); err != nil {
				return err
			}
		}

		if err := rst.manager.TXEndCB(rst); err != nil {
			return err
		}

		return nil
	case txstatus.TXERR:
		fallthrough
	case txstatus.TXACT:
		if replyCl {
			if err := rst.Cl.Send(&pgproto3.ReadyForQuery{
				TxStatus: byte(rst.txStatus),
			}); err != nil {
				return err
			}
		}
		return nil
	case txstatus.TXCONT:
		return nil
	default:
		err := fmt.Errorf("unknown tx status %v", rst.txStatus)
		return err
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
	if err := rst.manager.UnRouteCB(rst.Cl, shkey); err != nil {
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
func (rst *RelayStateImpl) UnrouteRoutes(routes []*routingstate.DataShardRoute) error {
	keys := make([]kr.ShardKey, len(routes))
	for ind, r := range routes {
		keys[ind] = r.Shkey
	}

	return rst.Unroute(keys)
}

// TODO : unit tests
func (rst *RelayStateImpl) UnRouteWithError(shkey []kr.ShardKey, errmsg error) error {
	_ = rst.manager.UnRouteWithError(rst.Cl, shkey, errmsg)
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

	if len(rst.Client().Server().Datashards()) != 1 {
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
func (rst *RelayStateImpl) ProcessExtendedBuffer(cmngr poolmgr.PoolMgr) error {

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

			if rst.PgprotoDebug() {
				if err := rst.Client().ReplyDebugNoticef("name %v, query %v, hash %d", q.Name, q.Query, hash); err != nil {
					return err
				}
			}

			fin, err := rst.PrepareRelayStepOnAnyRoute(cmngr)
			if err != nil {
				return err
			}

			/* TODO: refactor code to make this less ugly */
			saveTxStatus := rst.txStatus

			_, retMsg, err := rst.DeployPrepStmt(q.Name)
			if err != nil {
				return err
			}

			rst.SetTxStatus(saveTxStatus)

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

			phx := NewSimpleProtoStateHandler(rst.manager)

			def := rst.Client().PreparedStatementDefinitionByName(q.PreparedStatement)

			// We implicitly assume that there is always Execute after Bind for the same portal.
			// hovewer, postgresql protocol allows some more cases.
			if err := rst.Client().ReplyBindComplete(); err != nil {
				return err
			}

			rst.execute = func() error {
				return nil
			}

			if err := ProcQueryAdvanced(rst, def.Query, phx, func() error {
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

				if err := rst.PrepareRelayStep(cmngr); err != nil {
					return err
				}

				// hold route if appropriate

				if holdRoute {
					rst.HoldRouting()
				}

				// TODO: multi-shard statements
				if rst.bindRoute == nil {
					routes := rst.CurrentRoutes()
					if len(routes) == 1 {
						rst.bindRoute = routes[0]
					} else {
						return fmt.Errorf("failed to deploy prepared statement")
					}
				}

				rst.execute = func() error {
					err := rst.PrepareRelayStepOnHintRoute(cmngr, rst.bindRoute)
					if err != nil {
						return err
					}

					_, _, err = rst.DeployPrepStmt(q.PreparedStatement)
					if err != nil {
						return err
					}

					/* Case when no decribe stmt was issued before Execute+Sync*/
					if rst.saveBind != nil {
						rst.AddSilentQuery(rst.saveBind)
						// do not send saved bind twice
					}

					rst.AddQuery(&pgproto3.Execute{})

					rst.AddQuery(&pgproto3.Sync{})
					if _, _, err := rst.RelayFlush(true, true); err != nil {
						return err
					}

					// do not complete relay here yet
					return nil
				}

				return nil
			}, true /* cache parsing for prep statement */); err != nil {
				return err
			}

		case *pgproto3.Describe:
			// save txstatus because it may be overwritten if we have no backend connection
			saveTxStat := rst.TxStatus()

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

					err := rst.PrepareRelayStepOnHintRoute(cmngr, rst.bindRoute)
					if err != nil {
						return err
					}

					if _, _, err := rst.DeployPrepStmt(rst.lastBindName); err != nil {
						return err
					}

					// do not send saved bind twice
					if rst.saveBind == nil {
						// wtf?
						return fmt.Errorf("failed to describe statement, stmt was never deployed")
					}

					_, _, err = rst.RelayStep(rst.saveBind, false, false)
					if err != nil {
						return err
					}

					_, _, err = rst.RelayStep(q, false, false)
					if err != nil {
						return err
					}

					/* Here we close portal, so other clients can reuse it */
					_, _, err = rst.RelayStep(&pgproto3.Close{
						ObjectType: 'P',
					}, false, false)
					if err != nil {
						return err
					}

					_, unreplied, err := rst.RelayStep(&pgproto3.Sync{}, true, false)
					if err != nil {
						return err
					}

					for _, msg := range unreplied {
						spqrlog.Zero.Debug().Type("msg type", msg).Msg("desctibe portal unreplied message")
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
				spqrlog.Zero.Debug().
					Uint("client", rst.Client().ID()).
					Str("stmt-name", q.Name).
					Msg("Describe prep statement")

				fin, err := rst.PrepareRelayStepOnAnyRoute(cmngr)
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

			rst.SetTxStatus(saveTxStat)

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

	// no backend connection.
	// for example, parse + sync will cause so.
	// just reply rfq
	if rst.Client().Server() == nil {
		return rst.Client().ReplyRFQ(rst.TxStatus())
	} else {
		if len(rst.msgBuf) != 0 {
			rst.AddQuery(&pgproto3.Sync{})
			if _, _, err := rst.RelayFlush(true, true); err != nil {
				return err
			}
		}

		if err := rst.CompleteRelay(true); err != nil {
			return err
		}
	}

	return nil
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
				stm.Columns, _ = rst.Qr.SchemaCache().GetColumns(tableref.SchemaName, tableref.RelationName)
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
func (rst *RelayStateImpl) PrepareRelayStep(cmngr poolmgr.PoolMgr) error {
	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Str("user", rst.Client().Usr()).
		Str("db", rst.Client().DB()).
		Msg("preparing relay step for client")
	if rst.holdRouting {
		return nil
	}
	// txactive == 0 || activeSh == nil
	if !cmngr.ValidateReRoute(rst) {
		return nil
	}

	switch err := rst.Reroute(); err {
	case nil:
		return nil
	case ErrSkipQuery:
		if err := rst.Client().ReplyErr(err); err != nil {
			return err
		}
		return ErrSkipQuery
	case qrouter.MatchShardError:
		_ = rst.Client().ReplyErrMsgByCode(spqrerror.SPQR_NO_DATASHARD)
		return ErrSkipQuery
	default:
		rst.msgBuf = nil
		return err
	}
}

var noopCloseRouteFunc = func() error {
	return nil
}

// TODO : unit tests
func (rst *RelayStateImpl) PrepareRelayStepOnHintRoute(cmngr poolmgr.PoolMgr, route *routingstate.DataShardRoute) error {
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
	// alreasy has route, no need for any hint
	if !cmngr.ValidateReRoute(rst) {
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
	case qrouter.MatchShardError:
		_ = rst.Client().ReplyErrMsgByCode(spqrerror.SPQR_NO_DATASHARD)
		return ErrSkipQuery
	default:
		rst.msgBuf = nil
		return err
	}
}

// TODO : unit tests
func (rst *RelayStateImpl) PrepareRelayStepOnAnyRoute(cmngr poolmgr.PoolMgr) (func() error, error) {
	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Str("user", rst.Client().Usr()).
		Str("db", rst.Client().DB()).
		Msg("preparing relay step for client on any route")

	if rst.holdRouting {
		return noopCloseRouteFunc, nil
	}

	// txactive == 0 || activeSh == nil
	if !cmngr.ValidateReRoute(rst) {
		return noopCloseRouteFunc, nil
	}

	switch err := rst.RerouteToRandomRoute(); err {
	case nil:
		routes := rst.CurrentRoutes()
		return func() error {
			return rst.UnrouteRoutes(routes)
		}, nil
	case ErrSkipQuery:
		if err := rst.Client().ReplyErr(err); err != nil {
			return noopCloseRouteFunc, err
		}
		return noopCloseRouteFunc, ErrSkipQuery
	case qrouter.MatchShardError:
		_ = rst.Client().ReplyErrMsgByCode(spqrerror.SPQR_NO_DATASHARD)
		return noopCloseRouteFunc, ErrSkipQuery
	default:
		rst.msgBuf = nil
		return noopCloseRouteFunc, err
	}
}

// TODO : unit tests
func (rst *RelayStateImpl) ProcessMessageBuf(waitForResp, replyCl, completeRelay bool, cmngr poolmgr.PoolMgr) (bool, error) {
	if err := rst.PrepareRelayStep(cmngr); err != nil {
		return false, err
	}

	statistics.RecordStartTime(statistics.Shard, time.Now(), rst.Client().ID())

	if _, ok, err := rst.RelayFlush(waitForResp, replyCl); err != nil {
		return false, err
	} else {
		if completeRelay {
			if err := rst.CompleteRelay(replyCl); err != nil {
				return false, err
			}
		}
		return ok, nil
	}
}

// TODO : unit tests
func (rst *RelayStateImpl) ProcessMessage(
	msg pgproto3.FrontendMessage,
	waitForResp, replyCl bool,
	cmngr poolmgr.PoolMgr) error {
	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Msg("relay step: process message for client")
	if err := rst.PrepareRelayStep(cmngr); err != nil {
		return err
	}

	if _, _, err := rst.RelayStep(msg, waitForResp, replyCl); err != nil {
		if err := rst.CompleteRelay(replyCl); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
		return err
	}

	return rst.CompleteRelay(replyCl)
}

func (rst *RelayStateImpl) ConnMgr() poolmgr.PoolMgr {
	return rst.manager
}
