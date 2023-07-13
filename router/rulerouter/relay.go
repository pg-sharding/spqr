package rulerouter

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgproto3/v2"
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
	pgquery "github.com/pganalyze/pg_query_go/v4"
)

type RelayStateMgr interface {
	txstatus.TxStatusMgr

	Reset() error
	StartTrace()
	Flush()

	Reroute() error
	ShouldRetry(err error) bool
	Parse(query string) (parser.ParseState, string, error)

	AddQuery(q pgproto3.FrontendMessage)
	AddSilentQuery(q pgproto3.FrontendMessage)
	ActiveShards() []kr.ShardKey
	ActiveShardsReset()
	TxActive() bool

	RelayStep(msg pgproto3.FrontendMessage, waitForResp bool, replyCl bool) (txstatus.TXStatus, error)

	UnRouteWithError(shkey []kr.ShardKey, errmsg error) error
	CompleteRelay(replyCl bool) error
	Close() error
	Client() client.RouterClient

	ProcessMessage(msg pgproto3.FrontendMessage, waitForResp, replyCl bool, cmngr PoolMgr) error
	PrepareStatement(hash uint64, d server.PrepStmtDesc) error
	PrepareRelayStep(cmngr PoolMgr) error
	ProcessMessageBuf(waitForResp, replyCl bool, cmngr PoolMgr) (bool, error)
	RelayRunCommand(msg pgproto3.FrontendMessage, waitForResp bool, replyCl bool) error

	Sync(waitForResp, replyCl bool, cmngr PoolMgr) error

	RouterMode() config.RouterMode
}

type RelayStateImpl struct {
	txStatus   txstatus.TXStatus
	CopyActive bool

	activeShards   []kr.ShardKey
	TargetKeyRange kr.KeyRange

	traceMsgs          bool
	WorldShardFallback bool
	routerMode         config.RouterMode

	routingState qrouter.RoutingState

	Qr      qrouter.QueryRouter
	qp      parser.QParser
	stmts   *pgquery.ParseResult
	plainQ  string
	Cl      client.RouterClient
	manager PoolMgr

	maintain_params bool

	msgBuf  []pgproto3.FrontendMessage
	smsgBuf []pgproto3.FrontendMessage
}

func (rst *RelayStateImpl) SetTxStatus(status txstatus.TXStatus) {
	rst.txStatus = status
}

func (rst *RelayStateImpl) Client() client.RouterClient {
	return rst.Cl
}

func (rst *RelayStateImpl) TxStatus() txstatus.TXStatus {
	return rst.txStatus
}

func (rst *RelayStateImpl) PrepareStatement(hash uint64, d server.PrepStmtDesc) error {
	if rst.Cl.Server().HasPrepareStatement(hash) {
		return nil
	}
	if err := rst.RelayParse(&pgproto3.Parse{
		Name:  d.Name,
		Query: d.Query,
	}, false, false); err != nil {
		if rst.ShouldRetry(err) {
			return fmt.Errorf("retry logic for prepared statements is not implemented")
		}
		return err
	}

	if _, err := rst.RelayStep(&pgproto3.Sync{}, true, false); err != nil {
		if rst.ShouldRetry(err) {
			return fmt.Errorf("retry logic for prepared statements is not implemented")
		}
		return err
	}

	// dont need to complete relay because tx state didt changed
	rst.Cl.Server().PrepareStatement(hash)
	return nil
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

	if err := rst.manager.UnRouteCB(rst.Cl, rst.activeShards); err != nil {
		return err
	}

	rst.activeShards = nil
	for _, shr := range routes {
		rst.activeShards = append(rst.activeShards, shr.Shkey)
	}
	// TDB: hide under setting
	if err := rst.Cl.ReplyDebugNoticef("matched datashard routes %+v", routes); err != nil {
		return err
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

func (rst *RelayStateImpl) Reroute() error {
	_ = rst.Cl.ReplyDebugNotice("rerouting the client connection")

	span := opentracing.StartSpan("reroute")
	defer span.Finish()
	span.SetTag("user", rst.Cl.Usr())
	span.SetTag("db", rst.Cl.DB())

	spqrlog.Zero.Debug().
		Uint("client", spqrlog.GetPointer(rst.Client())).
		Interface("statement", rst.stmts).
		Msg("rerouting client")

	routingState, err := rst.Qr.Route(context.TODO(), rst.stmts)
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

	//span.SetTag("shard_routes", routingState)
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
		_, _, err = rst.Cl.ProcQuery(query, true, false)
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

func (rst *RelayStateImpl) RelayParse(v pgproto3.FrontendMessage, waitForResp bool, replyCl bool) error {
	return rst.Cl.ProcParse(v, waitForResp, replyCl)
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

			var v *pgproto3.Query
			v, buff = buff[0].(*pgproto3.Query), buff[1:]
			spqrlog.Zero.Debug().
				Bool("waitForResp", waitForResp).
				Bool("replyCl", replyCl).
				Msg("flushing")
			if txst, txok, err = rst.Cl.ProcQuery(v, waitForResp, replyCl); err != nil {
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

func (rst *RelayStateImpl) RelayStep(msg pgproto3.FrontendMessage, waitForResp bool, replyCl bool) (txstatus.TXStatus, error) {
	if !rst.TxActive() {
		if err := rst.manager.TXBeginCB(rst); err != nil {
			return 0, err
		}
	}

	bt, _, err := rst.Cl.ProcQuery(msg, waitForResp, replyCl)
	if err != nil {
		rst.SetTxStatus(txstatus.TXERR)
		return txstatus.TXERR, err
	}
	rst.SetTxStatus(bt)
	return rst.txStatus, nil
}

func (rst *RelayStateImpl) ShouldRetry(err error) bool {
	return false
}

func (rst *RelayStateImpl) CompleteRelay(replyCl bool) error {
	if rst.CopyActive {
		return nil
	}
	statistics.RecordFinishedTransaction(time.Now(), rst.Client().ID()) // end time measurement --------------------

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

func (rst *RelayStateImpl) Parse(query string) (parser.ParseState, string, error) {
	state, comm, err := rst.qp.Parse(query)
	rst.stmts, _ = rst.qp.Stmt()
	rst.plainQ = query
	return state, comm, err
}

var _ RelayStateMgr = &RelayStateImpl{}

func (rst *RelayStateImpl) PrepareRelayStep(cmngr PoolMgr) error {
	spqrlog.Zero.Debug().
		Uint("client", spqrlog.GetPointer(rst.Client())).
		Str("user", rst.Client().Usr()).
		Str("db", rst.Client().DB()).
		Msg("reparing relay step for client")
	// txactive == 0 || activeSh == nil
	if !cmngr.ValidateReRoute(rst) {
		return nil
	}

	switch err := rst.Reroute(); err {
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
	if err := rst.PrepareRelayStep(cmngr); err != nil {
		return false, err
	}
	//time start shard-----------------------------------------------------------------------------------
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
	if err := rst.PrepareRelayStep(cmngr); err != nil {
		return err
	}

	if _, _, err := rst.RelayFlush(waitForResp, replyCl); err != nil {
		/* Relay flush completes relay */
		return err
	}

	if _, err := rst.RelayStep(&pgproto3.Sync{}, waitForResp, replyCl); err != nil {
		return err
	}
	return nil
}

func (rst *RelayStateImpl) ProcessMessage(msg pgproto3.FrontendMessage, waitForResp, replyCl bool, cmngr PoolMgr) error {
	spqrlog.Zero.Debug().
		Uint("client", spqrlog.GetPointer(&rst.Cl)).
		Msg("relay step: process message for client")
	if err := rst.PrepareRelayStep(cmngr); err != nil {
		return err
	}

	if _, err := rst.RelayStep(msg, waitForResp, replyCl); err != nil {
		if err := rst.CompleteRelay(replyCl); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return err
		}
		return err
	}

	if err := rst.CompleteRelay(replyCl); err != nil {
		return err
	}
	return nil
}
