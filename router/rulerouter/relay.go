package rulerouter

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/opentracing/opentracing-go"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/pg-sharding/spqr/router/client"
	"github.com/pg-sharding/spqr/router/parser"
	"github.com/pg-sharding/spqr/router/qrouter"
	"github.com/pg-sharding/spqr/router/server"
	"github.com/pg-sharding/spqr/router/statistics"
	"github.com/spaolacci/murmur3"
)

type RelayStateMgr interface {
	txstatus.TxStatusMgr

	Reset() error
	StartTrace()
	Flush()

	// Parse and analyze user query, and decide which shard routes
	// will participate in query execturion
	Reroute(params [][]byte) error
	// Acquire (prepare) connection to any random shard route
	// Without any user query analysis
	RerouteToRandomRoute() error
	ShouldRetry(err error) bool
	Parse(query string) (parser.ParseState, string, error)

	AddQuery(q pgproto3.FrontendMessage)
	AddSilentQuery(q pgproto3.FrontendMessage)
	ActiveShards() []kr.ShardKey
	ActiveShardsReset()
	TxActive() bool

	PgprotoDebug() bool

	RelayStep(msg pgproto3.FrontendMessage, waitForResp bool, replyCl bool) (txstatus.TXStatus, []pgproto3.BackendMessage, error)

	UnRouteWithError(shkey []kr.ShardKey, errmsg error) error
	Unroute(shkey []kr.ShardKey) error

	CompleteRelay(replyCl bool) error
	Close() error
	Client() client.RouterClient

	ProcessMessage(msg pgproto3.FrontendMessage, waitForResp, replyCl bool, cmngr PoolMgr) error
	PrepareStatement(hash uint64, d server.PrepStmtDesc) (server.PreparedStatementDescriptor, error)

	PrepareRelayStep(cmngr PoolMgr, parameters [][]byte) error
	PrepareRelayStepOnAnyRoute(cmngr PoolMgr) error

	ProcessMessageBuf(waitForResp, replyCl bool, cmngr PoolMgr) (bool, error)
	RelayRunCommand(msg pgproto3.FrontendMessage, waitForResp bool, replyCl bool) error

	Sync(waitForResp, replyCl bool, cmngr PoolMgr) error

	RouterMode() config.RouterMode

	AddExtendedProtocMessage(q pgproto3.FrontendMessage)
	ProcessExtendedBuffer(cmngr PoolMgr) error
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

	routingState qrouter.RoutingState

	Qr      qrouter.QueryRouter
	qp      parser.QParser
	plainQ  string
	Cl      client.RouterClient
	manager PoolMgr

	maintain_params bool

	msgBuf  []pgproto3.FrontendMessage
	smsgBuf []pgproto3.FrontendMessage

	// buffer of messages to process on Sync request
	xBuf []pgproto3.FrontendMessage
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

func (rst *RelayStateImpl) PrepareStatement(hash uint64, d server.PrepStmtDesc) (server.PreparedStatementDescriptor, error) {
	rst.Cl.ServerAcquireUse()
	defer rst.Cl.ServerReleaseUse()

	if ok, rd := rst.Cl.Server().HasPrepareStatement(hash); ok {
		return rd, nil
	}
	// Do not wait for result
	if err := rst.Client().FireMsg(&pgproto3.Parse{
		Name:  d.Name,
		Query: d.Query,
	}); err != nil {
		return server.PreparedStatementDescriptor{}, err
	}

	err := rst.Client().FireMsg(&pgproto3.Describe{
		ObjectType: 'S',
		Name:       d.Name,
	})
	if err != nil {
		return server.PreparedStatementDescriptor{}, err
	}

	spqrlog.Zero.Debug().Str("client", rst.Client().ID()).Msg("syncing connection")

	_, unreplied, err := rst.RelayStep(&pgproto3.Sync{}, true, false)
	if err != nil {
		return server.PreparedStatementDescriptor{}, err
	}

	rd := server.PreparedStatementDescriptor{}

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
			spqrlog.Zero.Debug().Str("client", rst.Client().ID()).Interface("type", msg).Msg("unreplied pgproto message")
		}
	}

	// dont need to complete relay because tx state didt changed
	rst.Cl.Server().PrepareStatement(hash, rd)
	return rd, nil
}

func NewRelayState(qr qrouter.QueryRouter, client client.RouterClient, manager PoolMgr, rcfg *config.Router) RelayStateMgr {
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

func (rst *RelayStateImpl) RouterMode() config.RouterMode {
	return rst.routerMode
}

func (rst *RelayStateImpl) Close() error {
	defer rst.Cl.Close()
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

func (rst *RelayStateImpl) Reset() error {
	rst.activeShards = nil
	rst.txStatus = txstatus.TXIDLE

	_ = rst.Cl.Reset()

	return rst.Cl.Unroute()
}

func (rst *RelayStateImpl) StartTrace() {
	rst.traceMsgs = true
}

func (rst *RelayStateImpl) Flush() {
	rst.msgBuf = nil
	rst.traceMsgs = false
}

var ErrSkipQuery = fmt.Errorf("wait for a next query")

func (rst *RelayStateImpl) procRoutes(routes []*qrouter.DataShardRoute) error {
	// if there is no routes configurted, there is nowhere to route to
	if len(routes) == 0 {
		return qrouter.MatchShardError
	}

	spqrlog.Zero.Debug().
		Uint("relay state", spqrlog.GetPointer(rst)).
		Msg("unroute previous connections")

	if err := rst.manager.UnRouteCB(rst.Cl, rst.activeShards); err != nil {
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
			Str("client", rst.Cl.ID()).
			Msg("client encounter while initialing server connection")

		_ = rst.Reset()
		return err
	}

	return nil
}

func (rst *RelayStateImpl) Reroute(params [][]byte) error {
	_ = rst.Cl.ReplyDebugNotice("rerouting the client connection")

	span := opentracing.StartSpan("reroute")
	defer span.Finish()
	span.SetTag("user", rst.Cl.Usr())
	span.SetTag("db", rst.Cl.DB())

	spqrlog.Zero.Debug().
		Uint("client", spqrlog.GetPointer(rst.Client())).
		Interface("statement", rst.qp.Stmt()).
		Interface("params", params).
		Interface("statement", rst.qp.Stmt()).
		Msg("rerouting the client connection, resolving shard")

	routingState, err := rst.Qr.Route(context.TODO(), rst.qp.Stmt(), params)
	if err != nil {
		return fmt.Errorf("error processing query '%v': %v", rst.plainQ, err)
	}
	rst.routingState = routingState
	switch v := routingState.(type) {
	case qrouter.MultiMatchState:
		if rst.TxActive() {
			return fmt.Errorf("ddl is forbidden inside multi-shard transition")
		}
		return rst.procRoutes(rst.Qr.DataShardsRoutes())
	case qrouter.ShardMatchState:
		// TBD: do it better
		return rst.procRoutes(v.Routes)
	case qrouter.SkipRoutingState:
		return ErrSkipQuery
	case qrouter.WorldRouteState:
		if !rst.WorldShardFallback {
			return err
		}

		// fallback to execute query on world datashard (s)
		_, _ = rst.RerouteWorld()
		if err := rst.ConnectWorld(); err != nil {
			_ = rst.UnRouteWithError(nil, fmt.Errorf("failed to fallback on world datashard: %w", err))
			return err
		}

		return nil
	default:
		return fmt.Errorf("unexpected route state %T", v)
	}
}

func (rst *RelayStateImpl) RerouteToRandomRoute() error {
	_ = rst.Cl.ReplyDebugNotice("rerouting the client connection")

	span := opentracing.StartSpan("reroute")
	defer span.Finish()
	span.SetTag("user", rst.Cl.Usr())
	span.SetTag("db", rst.Cl.DB())

	spqrlog.Zero.Debug().
		Uint("client", spqrlog.GetPointer(rst.Client())).
		Interface("statement", rst.qp.Stmt()).
		Msg("rerouting the client connection, resolving shard")

	routes := rst.Qr.DataShardsRoutes()
	if len(routes) == 0 {
		return fmt.Errorf("no routes configured")
	}

	routingState := qrouter.ShardMatchState{
		Routes: []*qrouter.DataShardRoute{routes[rand.Int()%len(routes)]},
	}
	rst.routingState = routingState

	return rst.procRoutes(routingState.Routes)

}

func (rst *RelayStateImpl) RerouteWorld() ([]*qrouter.DataShardRoute, error) {
	span := opentracing.StartSpan("reroute to world")
	defer span.Finish()
	span.SetTag("user", rst.Cl.Usr())
	span.SetTag("db", rst.Cl.DB())

	shardRoutes := rst.Qr.WorldShardsRoutes()

	if len(shardRoutes) == 0 {
		_ = rst.manager.UnRouteWithError(rst.Cl, nil, qrouter.MatchShardError)
		return nil, qrouter.MatchShardError
	}

	if err := rst.manager.UnRouteCB(rst.Cl, rst.activeShards); err != nil {
		return nil, err
	}

	rst.activeShards = nil
	for _, shr := range shardRoutes {
		rst.activeShards = append(rst.activeShards, shr.Shkey)
	}

	return shardRoutes, nil
}

func (rst *RelayStateImpl) Connect(shardRoutes []*qrouter.DataShardRoute) error {
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
		Uint("client", spqrlog.GetPointer(rst.Cl)).
		Msg("route client to datashard")

	if err := rst.manager.RouteCB(rst.Cl, rst.activeShards); err != nil {
		return err
	}
	if rst.maintain_params {
		query := rst.Cl.ConstructClientParams()
		spqrlog.Zero.Debug().
			Uint("client", spqrlog.GetPointer(rst.Cl)).
			Str("query", query.String).
			Msg("setting params for client")
		_, _, _, err = rst.Cl.ProcQuery(query, true, false)
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

func (rst *RelayStateImpl) RelayCommand(v pgproto3.FrontendMessage, waitForResp bool, replyCl bool) error {
	if !rst.TxActive() {
		if err := rst.manager.TXBeginCB(rst); err != nil {
			return err
		}
		rst.txStatus = txstatus.TXACT
	}

	return rst.Cl.ProcCommand(v, waitForResp, replyCl)
}

func (rst *RelayStateImpl) RelayRunCommand(msg pgproto3.FrontendMessage, waitForResp bool, replyCl bool) error {
	return rst.Cl.ProcCommand(msg, waitForResp, replyCl)
}

func (rst *RelayStateImpl) RelayFlush(waitForResp bool, replyCl bool) (txstatus.TXStatus, bool, error) {
	spqrlog.Zero.Debug().
		Uint("client", spqrlog.GetPointer(rst.Client())).
		Msg("flushing message buffer")

	ok := true

	flusher := func(buff []pgproto3.FrontendMessage, waitForResp, replyCl bool) (txstatus.TXStatus, error) {

		var txst txstatus.TXStatus
		var err error
		var txok bool

		for len(buff) > 0 {
			if !rst.TxActive() {
				if err := rst.manager.TXBeginCB(rst); err != nil {
					return 0, err
				}
			}

			var v pgproto3.FrontendMessage
			v, buff = buff[0], buff[1:]
			spqrlog.Zero.Debug().
				Bool("waitForResp", waitForResp).
				Bool("replyCl", replyCl).
				Msg("flushing")
			if txst, _, txok, err = rst.Cl.ProcQuery(v, waitForResp, replyCl); err != nil {
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

	buf := rst.smsgBuf
	rst.smsgBuf = nil

	if txst, err = flusher(buf, true, false); err != nil {
		return txst, false, err
	}

	buf = rst.msgBuf
	rst.msgBuf = nil

	if txst, err = flusher(buf, waitForResp, replyCl); err != nil {
		return txst, false, err
	}

	if err := rst.CompleteRelay(replyCl); err != nil {
		return txstatus.TXERR, false, err
	}

	return txst, ok, nil
}

func (rst *RelayStateImpl) RelayStep(msg pgproto3.FrontendMessage, waitForResp bool, replyCl bool) (txstatus.TXStatus, []pgproto3.BackendMessage, error) {
	if !rst.TxActive() {
		if err := rst.manager.TXBeginCB(rst); err != nil {
			return 0, nil, err
		}
	}

	bt, unreplied, _, err := rst.Cl.ProcQuery(msg, waitForResp, replyCl)
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
		Uint("client", spqrlog.GetPointer(rst.Client())).
		Str("txstatus", rst.txStatus.String()).
		Msg("complete relay iter")

	switch rst.routingState.(type) {
	case qrouter.MultiMatchState:
		// TODO: explicitly forbid transaction, or hadnle it properly
		spqrlog.Zero.Debug().Msg("unroute multishard route")
		if replyCl {
			if err := rst.Cl.Send(&pgproto3.ReadyForQuery{
				TxStatus: byte(txstatus.TXIDLE),
			}); err != nil {
				return err
			}
		}

		return rst.manager.TXEndCB(rst)
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

func (rst *RelayStateImpl) Unroute(shkey []kr.ShardKey) error {
	return rst.manager.UnRouteCB(rst.Cl, shkey)
}

func (rst *RelayStateImpl) UnRouteWithError(shkey []kr.ShardKey, errmsg error) error {
	_ = rst.manager.UnRouteWithError(rst.Cl, shkey, errmsg)
	return rst.Reset()
}

func (rst *RelayStateImpl) AddQuery(q pgproto3.FrontendMessage) {
	spqrlog.Zero.Debug().
		Uint("client", spqrlog.GetPointer(rst.Client())).
		Type("message-type", q).
		Msg("client relay: adding message to message buffer")
	rst.msgBuf = append(rst.msgBuf, q)
}

func (rst *RelayStateImpl) AddSilentQuery(q pgproto3.FrontendMessage) {
	spqrlog.Zero.Debug().
		Interface("query", q).
		Msg("adding silent query")
	rst.smsgBuf = append(rst.smsgBuf, q)
}

func (rst *RelayStateImpl) AddExtendedProtocMessage(q pgproto3.FrontendMessage) {
	spqrlog.Zero.Debug().
		Interface("query", q).
		Msg("adding extended protocol message")
	rst.xBuf = append(rst.xBuf, q)
}

func (rst *RelayStateImpl) ProcessExtendedBuffer(cmngr PoolMgr) error {

	spqrlog.Zero.Debug().
		Str("client", rst.Client().ID()).
		Int("xBuf", len(rst.xBuf)).
		Msg("process extended buffer")

	saveBind := &pgproto3.Bind{}
	var lastBindQuery string

	for _, msg := range rst.xBuf {
		switch q := msg.(type) {
		case *pgproto3.Parse:

			hash := murmur3.Sum64([]byte(q.Query))
			spqrlog.Zero.Debug().
				Str("name", q.Name).
				Str("query", q.Query).
				Uint64("hash", hash).
				Str("client", rst.Client().ID()).
				Msg("Parsing prepared statement")

			if rst.PgprotoDebug() {
				if err := rst.Client().ReplyDebugNoticef("name %v, query %v, hash %d", q.Name, q.Query, hash); err != nil {
					return err
				}
			}
			rst.Client().StorePreparedStatement(q.Name, q.Query)
			// simply reply witch ok parse complete
			if err := rst.Client().ReplyParseComplete(); err != nil {
				return err
			}
		case *pgproto3.Bind:
			spqrlog.Zero.Debug().
				Str("name", q.PreparedStatement).
				Str("client", rst.Client().ID()).
				Msg("Binding prepared statement")

			saveBind.DestinationPortal = q.DestinationPortal

			lastBindQuery := rst.Client().PreparedStatementQueryByName(q.PreparedStatement)
			hash := murmur3.Sum64([]byte(lastBindQuery))

			saveBind.PreparedStatement = fmt.Sprintf("%d", hash)
			saveBind.ParameterFormatCodes = q.ParameterFormatCodes
			saveBind.Parameters = q.Parameters
			saveBind.ResultFormatCodes = q.ResultFormatCodes

			rst.Cl.Send(&pgproto3.BindComplete{})

		case *pgproto3.Describe:

			if q.ObjectType == 'P' {

				if err := rst.PrepareRelayStepOnAnyRoute(cmngr); err != nil {
					return err
				}

				err := rst.Client().FireMsg(q)
				if err != nil {
					return err
				}

				if _, _, err := rst.RelayStep(&pgproto3.Sync{}, true, true); err != nil {
					return err
				}

				if err := rst.Unroute([]kr.ShardKey{rst.routingState.(qrouter.ShardMatchState).Routes[0].Shkey}); err != nil {
					return err
				}
			} else {

				query := rst.Client().PreparedStatementQueryByName(q.Name)
				hash := murmur3.Sum64([]byte(query))

				spqrlog.Zero.Debug().
					Str("name", q.Name).
					Str("query", query).
					Uint64("hash", hash).
					Str("client", rst.Client().ID()).
					Msg("Describe prepared statement")

				if err := rst.PrepareRelayStepOnAnyRoute(cmngr); err != nil {
					return err
				}

				q.Name = fmt.Sprintf("%d", hash)
				rd, err := rst.PrepareStatement(hash, server.PrepStmtDesc{
					Name:  q.Name,
					Query: query,
				})

				if err != nil {
					return err
				}

				if err := rst.Client().Send(&rd.ParamDesc); err != nil {
					return err
				}
				if err := rst.Client().Send(&rd.RowDesc); err != nil {
					return err
				}

				if err := rst.Unroute([]kr.ShardKey{rst.routingState.(qrouter.ShardMatchState).Routes[0].Shkey}); err != nil {
					return err
				}
			}

		case *pgproto3.Execute:
			spqrlog.Zero.Debug().
				Str("client", rst.Client().ID()).
				Msg("Execute prepared statement")

			_, _, err := rst.qp.Parse(lastBindQuery)
			if err != nil {
				return err
			}

			if err := rst.PrepareRelayStep(cmngr, saveBind.Parameters); err != nil {
				return err
			}

			statistics.RecordStartTime(statistics.Shard, time.Now(), rst.Client().ID())

			if _, _, err := rst.RelayFlush(true, true); err != nil {
				return err
			}

		case *pgproto3.Close:
			//
		default:
			panic(fmt.Sprintf("unexpected query type %v", msg))
		}
	}

	rst.Client().Send(&pgproto3.ReadyForQuery{
		TxStatus: byte(rst.TxStatus()),
	})

	rst.xBuf = nil
	return nil
}

func (rst *RelayStateImpl) Parse(query string) (parser.ParseState, string, error) {
	state, comm, err := rst.qp.Parse(query)
	rst.plainQ = query
	return state, comm, err
}

var _ RelayStateMgr = &RelayStateImpl{}

func (rst *RelayStateImpl) PrepareRelayStep(cmngr PoolMgr, parameters [][]byte) error {
	spqrlog.Zero.Debug().
		Uint("client", spqrlog.GetPointer(rst.Client())).
		Str("user", rst.Client().Usr()).
		Str("db", rst.Client().DB()).
		Msg("preparing relay step for client")
	// txactive == 0 || activeSh == nil
	if !cmngr.ValidateReRoute(rst) {
		return nil
	}

	switch err := rst.Reroute(parameters); err {
	case nil:
		return nil
	case ErrSkipQuery:
		if err := rst.Client().ReplyErrMsg(err.Error()); err != nil {
			return err
		}
		return ErrSkipQuery
	case qrouter.MatchShardError:
		_ = rst.Client().ReplyErrMsg("failed to match any datashard")
		return ErrSkipQuery
	case qrouter.ParseError:
		_ = rst.Client().ReplyErrMsg("skip executing this query, wait for next")
		return ErrSkipQuery
	default:
		_ = rst.UnRouteWithError(nil, err)
		rst.msgBuf = nil
		rst.smsgBuf = nil
		return err
	}
}

func (rst *RelayStateImpl) PrepareRelayStepOnAnyRoute(cmngr PoolMgr) error {
	spqrlog.Zero.Debug().
		Uint("client", spqrlog.GetPointer(rst.Client())).
		Str("user", rst.Client().Usr()).
		Str("db", rst.Client().DB()).
		Msg("preparing relay step for client on any route")
	// txactive == 0 || activeSh == nil
	if !cmngr.ValidateReRoute(rst) {
		return nil
	}

	switch err := rst.RerouteToRandomRoute(); err {
	case nil:
		return nil
	case ErrSkipQuery:
		if err := rst.Client().ReplyErrMsg(err.Error()); err != nil {
			return err
		}
		return ErrSkipQuery
	case qrouter.MatchShardError:
		_ = rst.Client().ReplyErrMsg("failed to match any datashard")
		return ErrSkipQuery
	case qrouter.ParseError:
		_ = rst.Client().ReplyErrMsg("skip executing this query, wait for next")
		return ErrSkipQuery
	default:
		_ = rst.UnRouteWithError(nil, err)
		rst.msgBuf = nil
		rst.smsgBuf = nil
		return err
	}
}

func (rst *RelayStateImpl) ProcessMessageBuf(waitForResp, replyCl bool, cmngr PoolMgr) (bool, error) {
	if err := rst.PrepareRelayStep(cmngr, nil); err != nil {
		return false, err
	}

	statistics.RecordStartTime(statistics.Shard, time.Now(), rst.Client().ID())

	if _, ok, err := rst.RelayFlush(waitForResp, replyCl); err != nil {
		return false, err
	} else {
		return ok, nil
	}
}

func (rst *RelayStateImpl) Sync(waitForResp, replyCl bool, cmngr PoolMgr) error {
	spqrlog.Zero.Debug().
		Uint("client", spqrlog.GetPointer(rst.Client())).
		Msg("client relay exeсuting sync for client")

	// if we have no active connections, we have noting to sync
	if !cmngr.ConnectionActive(rst) {
		return rst.Client().ReplyRFQ()
	}
	if err := rst.PrepareRelayStep(cmngr, nil); err != nil {
		return err
	}

	if _, _, err := rst.RelayFlush(waitForResp, replyCl); err != nil {
		/* Relay flush completes relay */
		return err
	}

	if _, _, err := rst.RelayStep(&pgproto3.Sync{}, waitForResp, replyCl); err != nil {
		return err
	}
	return nil
}

func (rst *RelayStateImpl) ProcessMessage(msg pgproto3.FrontendMessage, waitForResp, replyCl bool, cmngr PoolMgr) error {
	spqrlog.Zero.Debug().
		Uint("client", spqrlog.GetPointer(&rst.Cl)).
		Msg("relay step: process message for client")
	if err := rst.PrepareRelayStep(cmngr, nil); err != nil {
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
