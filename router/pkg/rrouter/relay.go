package rrouter

import (
	"context"
	"fmt"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/router/pkg/parser"

	"github.com/jackc/pgproto3/v2"
	"github.com/opentracing/opentracing-go"
	"golang.org/x/xerrors"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/router/pkg/client"
	"github.com/pg-sharding/spqr/router/pkg/qrouter"
	"github.com/pg-sharding/spqr/router/pkg/server"
)

type RelayStateInteractor interface {
	Reset() error
	StartTrace()
	Flush()
	Reroute() error
	ShouldRetry(err error) bool
	Parse(q *pgproto3.Query) (parser.ParseState, error)
	AddQuery(q pgproto3.Query)
	AddSilentQuery(q pgproto3.Query)
	ActiveShards() []kr.ShardKey
	ActiveShardsReset()
	TxActive() bool
	SetTxStatus(status conn.TXStatus)
	RelayStep(msg pgproto3.FrontendMessage, waitForResp bool, replyCl bool) (conn.TXStatus, error)
	UnRouteWithError(shkey []kr.ShardKey, errmsg error) error
	CompleteRelay(replyCl bool) error
	Close() error
	Client() client.RouterClient
	ProcessMessage(msg pgproto3.FrontendMessage, waitForResp, replyCl bool, cmngr ConnManager) error
	PrepareStatement(hash uint64, d server.PrepStmtDesc) error
	PrepareRelayStep(cl client.RouterClient, cmngr ConnManager) error
	ProcessMessageBuf(waitForResp, replyCl bool, cmngr ConnManager) (bool, error)
	RelayRunCommand(msg pgproto3.FrontendMessage, waitForResp bool, replyCl bool) error
	TxStatus() conn.TXStatus
}

type RelayStateImpl struct {
	txStatus   conn.TXStatus
	sync       int
	CopyActive bool

	activeShards   []kr.ShardKey
	TargetKeyRange kr.KeyRange

	traceMsgs bool

	routingState qrouter.RoutingState

	Qr      qrouter.QueryRouter
	Cl      client.RouterClient
	manager ConnManager

	msgBuf  []pgproto3.Query
	smsgBuf []pgproto3.Query
}

func (rst *RelayStateImpl) SetTxStatus(status conn.TXStatus) {
	//TODO implement me
	rst.txStatus = status
}

func (rst *RelayStateImpl) Client() client.RouterClient {
	//TODO implement me
	return rst.Cl
}

func (rst *RelayStateImpl) TxStatus() conn.TXStatus {
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
			// TODO: fix retry logic
		}
		return err
	}

	if _, err := rst.RelayStep(&pgproto3.Sync{}, true, false); err != nil {
		if rst.ShouldRetry(err) {
			// TODO: fix retry logic
		}
		return err
	}

	// dont need to complete relay because tx state dont changed
	rst.Cl.Server().PrepareStatement(hash)
	return nil
}

func NewRelayState(qr qrouter.QueryRouter, client client.RouterClient, manager ConnManager) RelayStateInteractor {
	return &RelayStateImpl{
		activeShards: nil,
		txStatus:     conn.TXIDLE,
		msgBuf:       nil,
		traceMsgs:    false,
		Qr:           qr,
		Cl:           client,
		manager:      manager,
	}
}

func (rst *RelayStateImpl) Close() error {
	if err := rst.manager.UnRouteCB(rst.Cl, rst.activeShards); err != client.NotRouted {
		spqrlog.Logger.PrintError(err)
		_ = rst.Cl.Close()
		return err
	}
	return rst.Cl.Close()
}

func (rst *RelayStateImpl) TxActive() bool {
	return rst.txStatus == conn.TXACT
}

func (rst *RelayStateImpl) ActiveShardsReset() {
	rst.activeShards = nil
}

func (rst *RelayStateImpl) ActiveShards() []kr.ShardKey {
	return rst.activeShards
}

func (rst *RelayStateImpl) Reset() error {
	rst.activeShards = nil
	rst.txStatus = conn.TXIDLE

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

var SkipQueryError = xerrors.New("wait for next query")

func (rst *RelayStateImpl) procRoutes(routes []*qrouter.DataShardRoute) error {
	//
	if len(routes) == 0 {
		return qrouter.MatchShardError
	}

	if err := rst.manager.UnRouteCB(rst.Cl, rst.activeShards); err != nil && err != client.NotRouted {
		spqrlog.Logger.PrintError(err)
		return err
	}

	rst.activeShards = nil
	for _, shr := range routes {
		rst.activeShards = append(rst.activeShards, shr.Shkey)
	}
	//
	if err := rst.Cl.ReplyNotice(fmt.Sprintf("matched datashard routes %+v", routes)); err != nil {
		return err
	}

	if err := rst.Connect(routes); err != nil {
		spqrlog.Logger.Errorf("encounter %w while initialing server connection", err)

		_ = rst.Reset()
		_ = rst.Cl.ReplyErrMsg(err.Error())
		return err
	}

	return nil
}

func (rst *RelayStateImpl) Reroute() error {
	_ = rst.Cl.ReplyNotice(fmt.Sprintf("rerouting your connection"))

	span := opentracing.StartSpan("reroute")
	defer span.Finish()
	span.SetTag("user", rst.Cl.Usr())
	span.SetTag("db", rst.Cl.DB())

	routingState, err := rst.Qr.Route(context.TODO())
	if err != nil {
		return err
	}
	rst.routingState = routingState
	_ = rst.Cl.ReplyNotice(fmt.Sprintf("rerouting state %T %v", routingState, err))

	switch v := routingState.(type) {
	case qrouter.MultiMatchState:
		if rst.TxActive() {
			return fmt.Errorf("ddl is forbidden inside multi-shard transation")
		}
		return rst.procRoutes(rst.Qr.DataShardsRoutes())
	case qrouter.ShardMatchState:
		return rst.procRoutes(v.Routes)
	case qrouter.SkipRoutingState:
		return SkipQueryError
	case qrouter.WorldRouteState:
		if !config.RouterConfig().RulesConfig.WorldShardFallback {
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
		serv, err = server.NewMultiShardServer(rst.Cl.Route().BeRule(), rst.Cl.Route().ServPool())
		if err != nil {
			return err
		}
	} else {
		_ = rst.Cl.ReplyNotice("initialize single datashard server conn")
		serv = server.NewShardServer(rst.Cl.Route().BeRule(), rst.Cl.Route().ServPool())
	}

	if err := rst.Cl.AssignServerConn(serv); err != nil {
		return err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG1, "route cl %s:%s to %v", rst.Cl.Usr(), rst.Cl.DB(), shardRoutes)

	if err := rst.manager.RouteCB(rst.Cl, rst.activeShards); err != nil {
		return err
	}

	query := rst.Cl.ConstructClientParams()
	spqrlog.Logger.Printf(spqrlog.DEBUG1, "setting user params %s", query.String)
	_, _, err = rst.Cl.ProcQuery(query, true, false)
	return err
}

func (rst *RelayStateImpl) ConnectWorld() error {
	_ = rst.Cl.ReplyNotice("initialize single datashard server conn")

	serv := server.NewShardServer(rst.Cl.Route().BeRule(), rst.Cl.Route().ServPool())
	if err := rst.Cl.AssignServerConn(serv); err != nil {
		return err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG1, "route cl %s:%s to world datashard", rst.Cl.Usr(), rst.Cl.DB())
	return rst.manager.RouteCB(rst.Cl, rst.activeShards)
}

func (rst *RelayStateImpl) RelayParse(v pgproto3.FrontendMessage, waitForResp bool, replyCl bool) error {
	return rst.Cl.ProcParse(v, waitForResp, replyCl)
}

func (rst *RelayStateImpl) RelayCommand(v pgproto3.FrontendMessage, waitForResp bool, replyCl bool) error {
	if !rst.TxActive() {
		if err := rst.manager.TXBeginCB(rst.Cl, rst); err != nil {
			return err
		}
		rst.txStatus = conn.TXACT
	}

	return rst.Cl.ProcCommand(v, waitForResp, replyCl)
}

func (rst *RelayStateImpl) RelayRunCommand(msg pgproto3.FrontendMessage, waitForResp bool, replyCl bool) error {
	return rst.Cl.ProcCommand(msg, waitForResp, replyCl)
}

func (rst *RelayStateImpl) RelayFlush(waitForResp bool, replyCl bool) (conn.TXStatus, bool, error) {
	spqrlog.Logger.Printf(spqrlog.DEBUG1, "flush message buff")

	ok := true

	flusher := func(buff []pgproto3.Query, waitForResp, replyCl bool) (conn.TXStatus, error) {

		var txst conn.TXStatus
		var err error
		var txok bool

		for len(buff) > 0 {
			if !rst.TxActive() {
				if err := rst.manager.TXBeginCB(rst.Cl, rst); err != nil {
					return 0, err
				}
			}

			var v pgproto3.Query
			v, buff = buff[0], buff[1:]
			spqrlog.Logger.Printf(spqrlog.DEBUG1, "flushing %+v waitForResp: %v replyCl: %v", v, waitForResp, replyCl)
			if txst, txok, err = rst.Cl.ProcQuery(&v, waitForResp, replyCl); err != nil {
				ok = false
				return conn.TXERR, err
			} else {
				ok = ok && txok
			}

			rst.SetTxStatus(txst)
		}

		return txst, nil
	}

	var txst conn.TXStatus
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
		return conn.TXERR, false, err
	}

	return txst, ok, nil
}

func (rst *RelayStateImpl) RelayStep(msg pgproto3.FrontendMessage, waitForResp bool, replyCl bool) (conn.TXStatus, error) {
	if !rst.TxActive() {
		if err := rst.manager.TXBeginCB(rst.Cl, rst); err != nil {
			return 0, err
		}
	}

	bt, _, err := rst.Cl.ProcQuery(msg, waitForResp, replyCl)
	if err != nil {
		rst.SetTxStatus(conn.TXERR)
		return conn.TXERR, err
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

	spqrlog.Logger.Printf(spqrlog.DEBUG1, "complete relay iter with TX %s", func(b conn.TXStatus) string {
		switch b {
		case conn.TXIDLE:
			return "idle"
		case conn.TXACT:
			return "active"
		case conn.TXERR:
			return "err"
		default:
			return "unknown"
		}
	}(rst.txStatus))

	switch rst.routingState.(type) {
	case qrouter.MultiMatchState:
		spqrlog.Logger.Printf(spqrlog.DEBUG1, "unroute multishard route")
		if replyCl {
			if err := rst.Cl.Send(&pgproto3.ReadyForQuery{
				TxStatus: byte(conn.TXIDLE),
			}); err != nil {
				return err
			}
		}

		return rst.manager.TXEndCB(rst.Cl, rst)
	}

	switch rst.txStatus {
	case conn.TXIDLE:
		if replyCl {
			if err := rst.Cl.Send(&pgproto3.ReadyForQuery{
				TxStatus: byte(rst.txStatus),
			}); err != nil {
				return err
			}
		}

		if err := rst.manager.TXEndCB(rst.Cl, rst); err != nil {
			return err
		}

		return nil
	case conn.TXERR:
		fallthrough
	case conn.TXACT:
		if replyCl {
			if err := rst.Cl.Send(&pgproto3.ReadyForQuery{
				TxStatus: byte(rst.txStatus),
			}); err != nil {
				return err
			}
		}
		return nil
	case conn.TXCONT:
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

func (rst *RelayStateImpl) AddQuery(q pgproto3.Query) {
	spqrlog.Logger.Printf(spqrlog.DEBUG1, "adding %T", q)
	rst.msgBuf = append(rst.msgBuf, q)
}

func (rst *RelayStateImpl) AddSilentQuery(q pgproto3.Query) {
	spqrlog.Logger.Printf(spqrlog.DEBUG1, "adding silent %T", q)
	rst.smsgBuf = append(rst.smsgBuf, q)
}

func (rst *RelayStateImpl) Parse(q *pgproto3.Query) (parser.ParseState, error) {
	return rst.Qr.Parse(q)
}

var _ RelayStateInteractor = &RelayStateImpl{}

func (rst *RelayStateImpl) PrepareRelayStep(cl client.RouterClient, cmngr ConnManager) error {
	spqrlog.Logger.Printf(spqrlog.DEBUG1, "preparing relay step for %s %s", cl.Usr(), cl.DB())
	// txactive == 0 || activeSh == nil
	if !cmngr.ValidateReRoute(rst) {
		return nil
	}

	switch err := rst.Reroute(); err {
	case nil:
		return nil
	case SkipQueryError:
		//_ = cl.ReplyErrMsg(fmt.Sprintf("skip executing this query, wait for next"))
		if err := cl.ReplyErrMsg(err.Error()); err != nil {
			return err
		}
		return SkipQueryError
	case qrouter.MatchShardError:
		_ = cl.ReplyErrMsg(fmt.Sprintf("failed to match any datashard"))
		return SkipQueryError
	case qrouter.ParseError:
		_ = cl.ReplyErrMsg(fmt.Sprintf("skip executing this query, wait for next"))
		return SkipQueryError
	default:
		_ = rst.UnRouteWithError(nil, err)
		rst.msgBuf = nil
		rst.smsgBuf = nil
		return err
	}
}

func (rst *RelayStateImpl) ProcessMessageBuf(waitForResp, replyCl bool, cmngr ConnManager) (bool, error) {
	if err := rst.PrepareRelayStep(rst.Cl, cmngr); err != nil {
		return false, err
	}

	if _, ok, err := rst.RelayFlush(waitForResp, replyCl); err != nil {
		if err := rst.CompleteRelay(replyCl); err != nil {
			spqrlog.Logger.PrintError(err)
			return false, err
		}
		return false, err
	} else {
		spqrlog.Logger.Printf(spqrlog.DEBUG1, "active shards are %+v", rst.ActiveShards)
		return ok, nil
	}
}

func (rst *RelayStateImpl) ProcessMessage(msg pgproto3.FrontendMessage, waitForResp, replyCl bool, cmngr ConnManager) error {
	if err := rst.PrepareRelayStep(rst.Cl, cmngr); err != nil {
		return err
	}

	if _, err := rst.RelayStep(msg, waitForResp, replyCl); err != nil {
		if err := rst.CompleteRelay(replyCl); err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
		return err
	}

	if err := rst.CompleteRelay(replyCl); err != nil {
		return err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG1, "active shards are %v", rst.ActiveShards)
	return nil
}
