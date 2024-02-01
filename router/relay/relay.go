package relay

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/pg-sharding/spqr/pkg/models/spqrerror"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/opentracing/opentracing-go"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/pg-sharding/spqr/router/client"
	"github.com/pg-sharding/spqr/router/parser"
	"github.com/pg-sharding/spqr/router/poolmgr"
	"github.com/pg-sharding/spqr/router/qrouter"
	"github.com/pg-sharding/spqr/router/route"
	"github.com/pg-sharding/spqr/router/routingstate"
	"github.com/pg-sharding/spqr/router/server"
	"github.com/pg-sharding/spqr/router/statistics"
	"github.com/spaolacci/murmur3"
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
	Parse(query string) (parser.ParseState, string, error)

	AddQuery(q pgproto3.FrontendMessage)
	AddSilentQuery(q pgproto3.FrontendMessage)
	TxActive() bool

	PgprotoDebug() bool

	RelayStep(msg pgproto3.FrontendMessage, waitForResp bool, replyCl bool) (txstatus.TXStatus, []pgproto3.BackendMessage, error)

	CompleteRelay(replyCl bool) error
	Close() error
	Client() client.RouterClient

	ProcessMessage(msg pgproto3.FrontendMessage, waitForResp, replyCl bool, cmngr poolmgr.PoolMgr) error
	PrepareStatement(hash uint64, d server.PrepStmtDesc) (shard.PreparedStatementDescriptor, error)

	PrepareRelayStep(cmngr poolmgr.PoolMgr) error
	PrepareRelayStepOnAnyRoute(cmngr poolmgr.PoolMgr) (func() error, error)
	PrepareRelayStepOnHintRoute(cmngr poolmgr.PoolMgr, route *routingstate.DataShardRoute) (func() error, error)

	ProcessMessageBuf(waitForResp, replyCl, completeRelay bool, cmngr poolmgr.PoolMgr) (bool, error)
	RelayRunCommand(msg pgproto3.FrontendMessage, waitForResp bool, replyCl bool) error

	Sync(waitForResp, replyCl bool, cmngr poolmgr.PoolMgr) error

	ProcQuery(query pgproto3.FrontendMessage, waitForResp bool, replyCl bool) (txstatus.TXStatus, []pgproto3.BackendMessage, bool, error)
	ProcCopy(query pgproto3.FrontendMessage) error

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

	bindRoute     *routingstate.DataShardRoute
	lastBindQuery string
	lastBindName  string

	saveBind *pgproto3.Bind

	// buffer of messages to process on Sync request
	xBuf []pgproto3.FrontendMessage
}

func NewRelayState(qr qrouter.QueryRouter, client client.RouterClient, manager poolmgr.PoolMgr, rcfg *config.Router) *RelayStateImpl {
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
	}
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
func (rst *RelayStateImpl) PrepareStatement(hash uint64, d server.PrepStmtDesc) (shard.PreparedStatementDescriptor, error) {
	rst.Cl.ServerAcquireUse()

	if ok, rd := rst.Cl.Server().HasPrepareStatement(hash); ok {
		rst.Cl.ServerReleaseUse()
		return rd, nil
	}

	rst.Cl.ServerReleaseUse()
	// used in following methods

	// Do not wait for result
	if err := rst.FireMsg(&pgproto3.Parse{
		Name:  d.Name,
		Query: d.Query,
	}); err != nil {
		return shard.PreparedStatementDescriptor{}, err
	}

	err := rst.FireMsg(&pgproto3.Describe{
		ObjectType: 'S',
		Name:       d.Name,
	})
	if err != nil {
		return shard.PreparedStatementDescriptor{}, err
	}

	spqrlog.Zero.Debug().Uint("client", rst.Client().ID()).Msg("syncing connection")

	_, unreplied, err := rst.RelayStep(&pgproto3.Sync{}, true, false)
	if err != nil {
		return shard.PreparedStatementDescriptor{}, err
	}

	rd := shard.PreparedStatementDescriptor{}

	for _, msg := range unreplied {
		switch q := msg.(type) {
		case *pgproto3.ParseComplete:
			// skip
		case *pgproto3.ErrorResponse:
			return rd, fmt.Errorf(q.Message)
		case *pgproto3.ParameterDescription:
			// copy
			rd.ParamDesc = *q
		case *pgproto3.RowDescription:
			// copy
			rd.RowDesc = *q
		default:
			spqrlog.Zero.Debug().Uint("client", rst.Client().ID()).Interface("type", msg).Msg("unreplied pgproto message")
		}
	}

	// dont need to complete relay because tx state didt changed
	rst.Cl.Server().PrepareStatement(hash, rd)
	return rd, nil
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
		Interface("statement", rst.qp.Stmt()).
		Interface("params", rst.Client().BindParams()).
		Msg("rerouting the client connection, resolving shard")

	routingState, err := rst.Qr.Route(context.TODO(), rst.qp.Stmt(), rst.Cl)

	if err != nil {
		return fmt.Errorf("error processing query '%v': %v", rst.plainQ, err)
	}
	rst.routingState = routingState
	switch v := routingState.(type) {
	case routingstate.MultiMatchState:
		if rst.TxActive() {
			return fmt.Errorf("ddl is forbidden inside multi-shard transition")
		}
		spqrlog.Zero.Debug().
			Uint("client", rst.Client().ID()).
			Err(err).
			Msgf("parsed multi-shard routing state")
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
		Interface("statement", rst.qp.Stmt()).
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
		Str("distribution", rst.Cl.Distribution()).
		Uint("client", rst.Client().ID()).
		Msg("connect client to datashard routes")

	if err := rst.manager.RouteCB(rst.Cl, rst.activeShards); err != nil {
		return err
	}
	if rst.maintain_params {
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
	rst.Client().RLock()
	defer rst.Client().RUnlock()

	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Interface("query", query).
		Msg("client process command")
	_ = rst.
		Client().
		ReplyDebugNotice(
			fmt.Sprintf("executing your query %v", query)) // TODO perfomance issue

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
			return fmt.Errorf(v.Message)
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

// TODO : unit tests
func (rst *RelayStateImpl) ProcCopy(query pgproto3.FrontendMessage) error {
	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Type("query-type", query).
		Msg("client process copy")
	_ = rst.Client().ReplyDebugNotice(fmt.Sprintf("executing your query %v", query)) // TODO perfomance issue
	rst.Client().RLock()
	defer rst.Client().RUnlock()
	return rst.Client().Server().Send(query)
}

// TODO : unit tests
func (rst *RelayStateImpl) ProcCopyComplete(query *pgproto3.FrontendMessage) error {
	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Type("query-type", query).
		Msg("client process copy end")
	rst.Client().RLock()
	defer rst.Client().RUnlock()
	if err := rst.Client().Server().Send(*query); err != nil {
		return err
	}

	for {
		if msg, err := rst.Client().Server().Receive(); err != nil {
			return err
		} else {
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
}

// TODO : unit tests
func (rst *RelayStateImpl) ProcQuery(query pgproto3.FrontendMessage, waitForResp bool, replyCl bool) (txstatus.TXStatus, []pgproto3.BackendMessage, bool, error) {
	rst.Client().RLock()
	defer rst.Client().RUnlock()

	server := rst.Client().Server()

	if server == nil {
		return txstatus.TXERR, nil, false, fmt.Errorf("client %p is out of transaction sync with router", rst.Client())
	}

	spqrlog.Zero.Debug().
		Uints("shards", shard.ShardIDs(server.Datashards())).
		Type("query-type", query).
		Msg("client process query")

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

			if err := func() error {
				for {
					cpMsg, err := rst.Client().Receive()
					if err != nil {
						return err
					}

					switch cpMsg.(type) {
					case *pgproto3.CopyData:
						if err := rst.ProcCopy(cpMsg); err != nil {
							return err
						}
					case *pgproto3.CopyDone, *pgproto3.CopyFail:
						if err := rst.ProcCopyComplete(&cpMsg); err != nil {
							return err
						}
						return nil
					default:
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

var MultiShardPrepStmtDeployError = fmt.Errorf("multishard prepared statement deploy is not supported")

// TODO : unit tests
func (rst *RelayStateImpl) DeployPrepStmt(qname string) (shard.PreparedStatementDescriptor, error) {
	query := rst.Client().PreparedStatementQueryByName(qname)
	hash := murmur3.Sum64([]byte(query))

	if len(rst.Client().Server().Datashards()) != 1 {
		return shard.PreparedStatementDescriptor{}, MultiShardPrepStmtDeployError
	}

	spqrlog.Zero.Debug().
		Str("name", qname).
		Str("query", query).
		Uint64("hash", hash).
		Uint("client", rst.Client().ID()).
		Uints("shards", shard.ShardIDs(rst.Client().Server().Datashards())).
		Msg("deploy prepared statement")

	// TODO: multi-shard statements
	if rst.bindRoute == nil {
		routes := rst.CurrentRoutes()
		if len(routes) == 1 {
			rst.bindRoute = routes[0]
		} else {
			return shard.PreparedStatementDescriptor{}, fmt.Errorf("failed to deploy prepared statement %s", query)
		}
	}

	name := fmt.Sprintf("%d", hash)
	return rst.PrepareStatement(hash, server.PrepStmtDesc{
		Name:  name,
		Query: query,
	})
}

// TODO : unit tests
func (rst *RelayStateImpl) FireMsg(query pgproto3.FrontendMessage) error {
	spqrlog.Zero.Debug().Interface("query", query).Msg("firing query")

	rst.Cl.ServerAcquireUse()
	defer rst.Cl.ServerReleaseUse()

	return rst.Cl.Server().Send(query)
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

	for _, msg := range rst.xBuf {
		switch q := msg.(type) {
		case *pgproto3.Parse:

			hash := murmur3.Sum64([]byte(q.Query))
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
			rst.Client().StorePreparedStatement(q.Name, q.Query)

			// tdb: fix this
			rst.plainQ = q.Query

			if err := rst.Client().ReplyParseComplete(); err != nil {
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

			rst.lastBindQuery = rst.Client().PreparedStatementQueryByName(q.PreparedStatement)

			// We implicitly assume that there is always Execute after Bind for the same portal.
			// hovewer, postgresql protocol allows some more cases.
			if err := rst.Client().ReplyBindComplete(); err != nil {
				return err
			}

			if err := ProcQueryAdvanced(rst, rst.lastBindQuery, phx, func() error {
				rst.saveBind = &pgproto3.Bind{}
				rst.saveBind.DestinationPortal = q.DestinationPortal

				rst.lastBindName = q.PreparedStatement
				hash := murmur3.Sum64([]byte(rst.lastBindQuery))

				rst.saveBind.PreparedStatement = fmt.Sprintf("%d", hash)
				rst.saveBind.ParameterFormatCodes = q.ParameterFormatCodes
				rst.Client().SetBindParams(q.Parameters)
				rst.saveBind.ResultFormatCodes = q.ResultFormatCodes
				rst.saveBind.Parameters = q.Parameters

				// Do not respond with BindComplete, as the relay step should take care of itself.

				if err := rst.PrepareRelayStep(cmngr); err != nil {
					return err
				}

				_, err := rst.DeployPrepStmt(q.PreparedStatement)
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

				rst.saveBind = nil
				rst.bindRoute = nil

				return nil
			}); err != nil {
				return err
			}

		case *pgproto3.Describe:
			if q.ObjectType == 'P' {
				spqrlog.Zero.Debug().
					Uint("client", rst.Client().ID()).
					Str("last-bind-name", rst.lastBindName).
					Msg("Describe portal")

				fin, err := rst.PrepareRelayStepOnHintRoute(cmngr, rst.bindRoute)
				if err != nil {
					return err
				}

				if _, err := rst.DeployPrepStmt(rst.lastBindName); err != nil {
					return err
				}

				rst.AddSilentQuery(rst.saveBind)
				// do not send saved bind twice
				rst.saveBind = nil
				rst.AddQuery(q)

				_, unreplied, err := rst.RelayStep(&pgproto3.Sync{}, true, false)
				if err != nil {
					return err
				}

				for _, msg := range unreplied {
					// https://www.postgresql.org/docs/current/protocol-flow.html
					switch qq := msg.(type) {
					case *pgproto3.RowDescription:
						// send to the client
						if err := rst.Client().Send(qq); err != nil {
							return err
						}
					case *pgproto3.NoData:
						// send to the client
						if err := rst.Client().Send(qq); err != nil {
							return err
						}
					default:
						// error out? panic? protoc violation?
						// no, just chill
					}
				}

				if err := fin(); err != nil {
					return err
				}

			} else {
				fin, err := rst.PrepareRelayStepOnAnyRoute(cmngr)
				if err != nil {
					return err
				}

				rd, err := rst.DeployPrepStmt(q.Name)
				if err != nil {
					return err
				}

				if err := rst.Client().Send(&rd.ParamDesc); err != nil {
					return err
				}
				if err := rst.Client().Send(&rd.RowDesc); err != nil {
					return err
				}

				if err := fin(); err != nil {
					return err
				}
			}
		case *pgproto3.Execute:
			spqrlog.Zero.Debug().
				Uint("client", rst.Client().ID()).
				Msg("Execute prepared statement, reset saved bind")
			/* actually done on bind */
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
		rst.AddQuery(&pgproto3.Sync{})
		if _, _, err := rst.RelayFlush(true, true); err != nil {
			return err
		}

		if err := rst.CompleteRelay(true); err != nil {
			return err
		}
	}

	return nil
}

// TODO : unit tests
func (rst *RelayStateImpl) Parse(query string) (parser.ParseState, string, error) {
	state, comm, err := rst.qp.Parse(query)
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
		Str("ds", rst.Client().Distribution()).
		Msg("preparing relay step for client")
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
	case qrouter.ParseError:
		_ = rst.Client().ReplyErrMsgByCode(spqrerror.SPQR_COMPLEX_QUERY)
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
func (rst *RelayStateImpl) PrepareRelayStepOnHintRoute(cmngr poolmgr.PoolMgr, route *routingstate.DataShardRoute) (func() error, error) {
	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Str("user", rst.Client().Usr()).
		Str("db", rst.Client().DB()).
		Int("curr routes len", len(rst.activeShards)).
		Interface("route", route).
		Msg("preparing relay step for client on target route")
	// txactive == 0 || activeSh == nil
	// alreasy has route, no need for any hint
	if !cmngr.ValidateReRoute(rst) {
		return noopCloseRouteFunc, nil
	}

	if route == nil {
		return noopCloseRouteFunc, fmt.Errorf("failed to use hint route")
	}

	switch err := rst.RerouteToTargetRoute(route); err {
	case nil:
		routes := rst.CurrentRoutes()
		return func() error {
			// drop connection if unneeded
			if rst.Cl.Server() != nil && rst.Cl.Server().TxStatus() == txstatus.TXIDLE {
				return rst.UnrouteRoutes(routes)
			}
			return nil
		}, nil
	case ErrSkipQuery:
		if err := rst.Client().ReplyErr(err); err != nil {
			return noopCloseRouteFunc, err
		}
		return noopCloseRouteFunc, ErrSkipQuery
	case qrouter.MatchShardError:
		_ = rst.Client().ReplyErrMsgByCode(spqrerror.SPQR_NO_DATASHARD)
		return noopCloseRouteFunc, ErrSkipQuery
	case qrouter.ParseError:
		_ = rst.Client().ReplyErrMsgByCode(spqrerror.SPQR_COMPLEX_QUERY)
		return noopCloseRouteFunc, ErrSkipQuery
	default:
		rst.msgBuf = nil
		return noopCloseRouteFunc, err
	}
}

// TODO : unit tests
func (rst *RelayStateImpl) PrepareRelayStepOnAnyRoute(cmngr poolmgr.PoolMgr) (func() error, error) {
	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Str("user", rst.Client().Usr()).
		Str("db", rst.Client().DB()).
		Msg("preparing relay step for client on any route")
	// txactive == 0 || activeSh == nil
	if !cmngr.ValidateReRoute(rst) {
		return noopCloseRouteFunc, nil
	}

	switch err := rst.RerouteToRandomRoute(); err {
	case nil:
		return func() error {
			return rst.UnrouteRoutes(rst.CurrentRoutes())
		}, nil
	case ErrSkipQuery:
		if err := rst.Client().ReplyErr(err); err != nil {
			return noopCloseRouteFunc, err
		}
		return noopCloseRouteFunc, ErrSkipQuery
	case qrouter.MatchShardError:
		_ = rst.Client().ReplyErrMsgByCode(spqrerror.SPQR_NO_DATASHARD)
		return noopCloseRouteFunc, ErrSkipQuery
	case qrouter.ParseError:
		_ = rst.Client().ReplyErrMsgByCode(spqrerror.SPQR_COMPLEX_QUERY)
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
func (rst *RelayStateImpl) Sync(waitForResp, replyCl bool, cmngr poolmgr.PoolMgr) error {
	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Msg("client relay exeсuting sync for client")

	// if we have no active connections, we have noting to sync
	if !cmngr.ConnectionActive(rst) {
		return rst.Client().ReplyRFQ(rst.TxStatus())
	}
	if err := rst.PrepareRelayStep(cmngr); err != nil {
		return err
	}

	if _, _, err := rst.RelayFlush(waitForResp, replyCl); err != nil {
		/* Relay flush completes relay */
		return err
	}

	if err := rst.CompleteRelay(replyCl); err != nil {
		return err
	}

	if _, _, err := rst.RelayStep(&pgproto3.Sync{}, waitForResp, replyCl); err != nil {
		return err
	}
	return nil
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
