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
	"github.com/pg-sharding/spqr/router/plan"
	"github.com/pg-sharding/spqr/router/poolmgr"
	"github.com/pg-sharding/spqr/router/qrouter"
	"github.com/pg-sharding/spqr/router/rmeta"
	"github.com/pg-sharding/spqr/router/route"
	"github.com/pg-sharding/spqr/router/server"
	"github.com/pg-sharding/spqr/router/statistics"
	"golang.org/x/exp/slices"
)

type RelayStateMgr interface {
	poolmgr.ConnectionKeeper
	route.RouteMgr

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

	ProcessMessage(msg pgproto3.FrontendMessage, waitForResp, replyCl bool) error
	PrepareStatement(hash uint64, d *prepstatement.PreparedStatementDefinition) (*prepstatement.PreparedStatementDescriptor, pgproto3.BackendMessage, error)

	PrepareRelayStep() error
	PrepareRelayStepOnAnyRoute() (func() error, error)
	PrepareRelayStepOnHintRoute(route *kr.ShardKey) error

	HoldRouting()
	UnholdRouting()

	/* process extended proto */
	ProcessMessageBuf(waitForResp, replyCl, completeRelay bool) error
	ProcQuery(query pgproto3.FrontendMessage, waitForResp bool, replyCl bool) ([]pgproto3.BackendMessage, bool, error)

	ProcCopyPrepare(ctx context.Context, stmt *lyx.Copy) (*pgcopy.CopyState, error)
	ProcCopy(ctx context.Context, data *pgproto3.CopyData, copyState *pgcopy.CopyState) ([]byte, error)
	ProcCopyComplete(query pgproto3.FrontendMessage) error

	ProcCommand(query pgproto3.FrontendMessage, waitForResp bool, replyCl bool) error

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
	txStatus txstatus.TXStatus

	traceMsgs    bool
	activeShards []kr.ShardKey

	routingState plan.Plan

	Qr      qrouter.QueryRouter
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
		txStatus:        txstatus.TXIDLE,
		msgBuf:          nil,
		traceMsgs:       false,
		Qr:              qr,
		Cl:              client,
		poolMgr:         manager,
		execute:         nil,
		savedPortalDesc: map[string]PortalDesc{},
		parseCache:      map[string]ParseCacheEntry{},
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

func (rst *RelayStateImpl) PoolMgr() poolmgr.PoolMgr {
	return rst.poolMgr
}

func (rst *RelayStateImpl) SetTxStatus(status txstatus.TXStatus) {
	rst.txStatus = status
	/* handle implicit transactions - rollback all local state for params */
	rst.Client().CleanupLocalSet()
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
		// dont need to complete relay because tx state didt changed
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

		if ok, _ := serv.HasPrepareStatement(hash, shardId); ok {
			continue
		}

		if err := serv.SendShard(&pgproto3.Parse{
			Name:          d.Name,
			Query:         d.Query,
			ParameterOIDs: d.ParameterOIDs,
		}, shardId); err != nil {
			return err
		}

		if err := serv.SendShard(&pgproto3.Describe{
			ObjectType: byte('S'),
			Name:       d.Name,
		}, shardId); err != nil {
			return err
		}

		if err := serv.SendShard(&pgproto3.Sync{}, shardId); err != nil {
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

	if err := serv.SendShard(bind, shardId); err != nil {
		return nil, err
	}

	if err := serv.SendShard(&pgproto3.Describe{
		ObjectType: byte('P'),
	}, shardId); err != nil {
		return nil, err
	}

	if err := serv.SendShard(&pgproto3.Close{ObjectType: byte('P')}, shardId); err != nil {
		return nil, err
	}

	if err := serv.SendShard(&pgproto3.Sync{}, shardId); err != nil {
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
	defer rst.Cl.Close()
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
	rst.txStatus = txstatus.TXIDLE

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

// TODO : unit tests
func (rst *RelayStateImpl) procRoutes(routes []*kr.ShardKey) error {
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
		rst.activeShards = append(rst.activeShards, *shr)
	}

	if config.RouterConfig().PgprotoDebug {
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
		Str("drb", rst.Client().DefaultRouteBehaviour()).
		Str("exec_on", rst.Client().ExecuteOn()).
		Msg("rerouting the client connection, resolving shard")

	var queryPlan plan.Plan

	if v := rst.Client().ExecuteOn(); v != "" {
		queryPlan = plan.ShardMatchState{
			Route: &kr.ShardKey{
				Name: v,
			},
		}
	} else {
		var err error
		queryPlan, err = rst.Qr.Route(context.TODO(), rst.qp.Stmt(), rst.Cl)
		if err != nil {
			return fmt.Errorf("error processing query '%v': %v", rst.plainQ, err)
		}
	}

	rst.routingState = queryPlan
	switch v := queryPlan.(type) {
	case plan.ScatterPlan:
		if rst.txStatus == txstatus.TXACT {
			return fmt.Errorf("cannot route in an active transaction")
		}
		spqrlog.Zero.Debug().
			Uint("client", rst.Client().ID()).
			Msgf("parsed ScatterPlan")
		return rst.procRoutes(rst.Qr.DataShardsRoutes())
	case plan.DDLState:
		spqrlog.Zero.Debug().
			Uint("client", rst.Client().ID()).
			Msgf("parsed DDLState")
		return rst.procRoutes(rst.Qr.DataShardsRoutes())
	case plan.ShardMatchState:
		// TBD: do it better
		return rst.procRoutes([]*kr.ShardKey{v.Route})
	case plan.SkipRoutingState:
		return ErrSkipQuery
	case plan.RandomMatchState:
		return rst.RerouteToRandomRoute()
	default:
		return fmt.Errorf("unexpected query plan %T", v)
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

	routingState := plan.ShardMatchState{
		Route: routes[rand.Int()%len(routes)],
	}
	rst.routingState = routingState

	return rst.procRoutes([]*kr.ShardKey{routingState.Route})
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

	routingState := plan.ShardMatchState{
		Route: route,
	}
	rst.routingState = routingState

	return rst.procRoutes([]*kr.ShardKey{route})
}

// TODO : unit tests
func (rst *RelayStateImpl) CurrentRoutes() []*kr.ShardKey {
	switch q := rst.routingState.(type) {
	case plan.ShardMatchState:
		return []*kr.ShardKey{q.Route}
	default:
		return nil
	}
}

// TODO : unit tests
func (rst *RelayStateImpl) Connect(shardRoutes []*kr.ShardKey) error {
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

	if err := rst.poolMgr.RouteCB(rst.Cl, rst.activeShards); err != nil {
		return err
	}

	/* take care of session param if we told to */
	if rst.Client().MaintainParams() {
		query := rst.Cl.ConstructClientParams()
		spqrlog.Zero.Debug().
			Uint("client", rst.Client().ID()).
			Str("query", query.String).
			Msg("setting params for client")
		_, _, err = rst.ProcQuery(query, true, false)
	}
	return err
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
	if rst.txStatus != txstatus.TXACT {
		if err := rst.poolMgr.TXBeginCB(rst); err != nil {
			return err
		}
		rst.txStatus = txstatus.TXACT
	}

	return rst.ProcCommand(v, waitForResp, replyCl)
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
	// Read delimiter from COPY options
	delimiter := byte('\t')
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

	/* If exeecute on is specified or explicit tx is going, no routing */
	if rst.Client().ExecuteOn() != "" || rst.txStatus == txstatus.TXACT {
		return &pgcopy.CopyState{
			Delimiter: delimiter,
			Attached:  true,
			ExpRoute:  &kr.ShardKey{},
		}, nil
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
		Delimiter:  delimiter,
		ExpRoute:   &kr.ShardKey{},
		Krs:        krs,
		RM:         rmeta.NewRoutingMetadataContext(rst.Cl, rst.Qr.Mgr()),
		TargetType: TargetType,
		HashFunc:   hashFunc,
	}, nil
}

// TODO : unit tests
func (rst *RelayStateImpl) ProcCopy(ctx context.Context, data *pgproto3.CopyData, cps *pgcopy.CopyState) ([]byte, error) {
	if cps.Attached {
		for _, sh := range rst.Client().Server().Datashards() {
			err := sh.Send(data)
			return nil, err
		}
		return nil, fmt.Errorf("metadata corrupted")
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
		currroute, err := cps.RM.DeparseKeyWithRangesInternal(ctx, values, cps.Krs)
		if err != nil {
			return nil, err
		}

		if currroute == nil {
			return nil, fmt.Errorf("multishard copy is not supported: %+v at line number %d %d %v", values[0], prevLine, i, b)
		}

		values = nil
		rowsMp[currroute.Name] = append(rowsMp[currroute.Name], data.Data[prevLine:i+1]...)
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
		if bts, ok := rowsMp[sh.Name()]; ok {
			err := sh.Send(&pgproto3.CopyData{Data: bts})
			if err != nil {
				return nil, err
			}
		}
	}

	// shouldn't exit from here
	return leftOvermsgData, nil
}

// TODO : unit tests
func (rst *RelayStateImpl) ProcCopyComplete(query pgproto3.FrontendMessage) error {
	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Type("query-type", query).
		Msg("client process copy end")
	if err := rst.Client().Server().Send(query); err != nil {
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
/* second param indicates if we should continue relay. */
func (rst *RelayStateImpl) ProcQuery(query pgproto3.FrontendMessage, waitForResp bool, replyCl bool) ([]pgproto3.BackendMessage, bool, error) {
	server := rst.Client().Server()

	if server == nil {
		rst.SetTxStatus(txstatus.TXERR)
		return nil, false, fmt.Errorf("client %p is out of transaction sync with router", rst.Client())
	}

	spqrlog.Zero.Debug().
		Uints("shards", shard.ShardIDs(server.Datashards())).
		Type("query-type", query).
		Msg("relay process query")

	if err := server.Send(query); err != nil {
		rst.SetTxStatus(txstatus.TXERR)
		return nil, false, err
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
		/* we do not alter txstatus here */
		return nil, true, nil
	}

	ok := true

	unreplied := make([]pgproto3.BackendMessage, 0)

	for {
		msg, err := server.Receive()
		if err != nil {
			rst.SetTxStatus(txstatus.TXERR)
			return nil, false, err
		}

		switch v := msg.(type) {
		case *pgproto3.CopyInResponse:
			// handle replyCl somehow
			err = rst.Client().Send(msg)
			if err != nil {
				rst.SetTxStatus(txstatus.TXERR)
				return nil, false, err
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
						if err := rst.ProcCopyComplete(cpMsg); err != nil {
							return err
						}
						return nil
					default:
						/* panic? */
					}
				}
			}(); err != nil {
				rst.SetTxStatus(txstatus.TXERR)
				return nil, false, err
			}
		case *pgproto3.ReadyForQuery:
			rst.SetTxStatus(txstatus.TXStatus(v.TxStatus))
			return unreplied, ok, nil
		case *pgproto3.ErrorResponse:
			if replyCl {
				err = rst.Client().Send(msg)
				if err != nil {
					rst.SetTxStatus(txstatus.TXERR)
					return nil, false, err
				}
			} else {
				unreplied = append(unreplied, msg)
			}
			ok = false
		// never resend these msgs
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
					rst.SetTxStatus(txstatus.TXERR)
					return nil, false, err
				}
			} else {
				unreplied = append(unreplied, msg)
			}
		}
	}
}

// TODO : unit tests
func (rst *RelayStateImpl) RelayFlush(waitForResp bool, replyCl bool) error {
	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Msg("flushing message buffer")

	flusher := func(buff []BufferedMessage, waitForResp, replyCl bool) error {
		for len(buff) > 0 {
			if rst.txStatus != txstatus.TXACT {
				if err := rst.poolMgr.TXBeginCB(rst); err != nil {
					return err
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

			if _, _, err := rst.ProcQuery(v.msg, waitForResp, resolvedReplyCl); err != nil {
				return err
			}

		}

		return nil
	}
	buf := rst.msgBuf
	rst.msgBuf = nil

	if err := flusher(buf, waitForResp, replyCl); err != nil {
		return err
	}

	return nil
}

// TODO : unit tests
func (rst *RelayStateImpl) RelayStep(msg pgproto3.FrontendMessage, waitForResp bool, replyCl bool) ([]pgproto3.BackendMessage, error) {
	if rst.txStatus != txstatus.TXACT {
		if err := rst.poolMgr.TXBeginCB(rst); err != nil {
			return nil, err
		}
	}

	unreplied, _, err := rst.ProcQuery(msg, waitForResp, replyCl)
	if err != nil {
		return unreplied, err
	}
	return unreplied, nil
}

func (rst *RelayStateImpl) ShouldRetry(err error) bool {
	return false
}

func (rst *RelayStateImpl) CompleteRelay(replyCl bool) error {
	statistics.RecordFinishedTransaction(time.Now(), rst.Client().ID())

	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Str("txstatus", rst.txStatus.String()).
		Msg("complete relay iter")

	switch rst.routingState.(type) {
	case plan.ScatterPlan:
		// TODO: explicitly forbid transaction, or hadnle it properly
		spqrlog.Zero.Debug().Msg("unroute multishard route")

		if err := rst.poolMgr.TXEndCB(rst); err != nil {
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

		if err := rst.poolMgr.TXEndCB(rst); err != nil {
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
func (rst *RelayStateImpl) UnrouteRoutes(routes []*kr.ShardKey) error {
	keys := make([]kr.ShardKey, len(routes))
	for ind, r := range routes {
		keys[ind] = *r
	}

	return rst.Unroute(keys)
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

			phx := NewSimpleProtoStateHandler()

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

				if err := rst.PrepareRelayStep(); err != nil {
					return err
				}

				// hold route if appropriate

				if holdRoute {
					rst.HoldRouting()
				}

				_, ok := rst.routingState.(plan.DDLState)
				if ok {
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

						if err := rst.RelayFlush(true, true); err != nil {
							return err
						}

						// do not complete relay here yet
						return nil
					}

					return nil
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
					err := rst.PrepareRelayStepOnHintRoute(rst.bindRoute)
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
					if err := rst.RelayFlush(true, true); err != nil {
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

					err := rst.PrepareRelayStepOnHintRoute(rst.bindRoute)
					if err != nil {
						return err
					}

					_, isDDL := rst.routingState.(plan.DDLState)
					if isDDL {
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
						break
					} else {
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
	}
	if len(rst.msgBuf) != 0 {
		rst.AddQuery(&pgproto3.Sync{})
		if err := rst.RelayFlush(true, true); err != nil {
			return err
		}
	}

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
					stm.Columns, _ = cptr.GetColumns(tableref.SchemaName, tableref.RelationName)
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
func (rst *RelayStateImpl) PrepareRelayStep() error {
	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Str("user", rst.Client().Usr()).
		Str("db", rst.Client().DB()).
		Msg("preparing relay step for client")
	if rst.holdRouting {
		return nil
	}
	// txactive == 0 || activeSh == nil
	if !rst.poolMgr.ValidateReRoute(rst) {
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
	case qrouter.MatchShardError:
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
func (rst *RelayStateImpl) ProcessMessageBuf(waitForResp, replyCl, completeRelay bool) error {
	if err := rst.PrepareRelayStep(); err != nil {
		return err
	}

	statistics.RecordStartTime(statistics.Shard, time.Now(), rst.Client().ID())

	if err := rst.RelayFlush(waitForResp, replyCl); err != nil {
		return err
	} else {
		if completeRelay {
			if err := rst.CompleteRelay(replyCl); err != nil {
				return err
			}
		}
		return nil
	}
}

// TODO : unit tests
func (rst *RelayStateImpl) ProcessMessage(
	msg pgproto3.FrontendMessage,
	waitForResp, replyCl bool) error {
	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Msg("relay step: process message for client")
	if err := rst.PrepareRelayStep(); err != nil {
		return err
	}

	if _, err := rst.RelayStep(msg, waitForResp, replyCl); err != nil {
		if err := rst.CompleteRelay(replyCl); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
		return err
	}

	return rst.CompleteRelay(replyCl)
}
