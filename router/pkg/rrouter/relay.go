package rrouter

import (
	"github.com/jackc/pgproto3"
	"github.com/opentracing/opentracing-go"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/router/pkg/qrouter"
	"github.com/wal-g/tracelog"
)

type RelayStateInteractor interface {
	Reset() error
	StartTrace()
	Flush()
	Reroute(q *pgproto3.Query) ([]qrouter.ShardRoute, error)
	ShouldRetry(err error) bool
}

type RelayStateImpl struct {
	TxActive bool

	ActiveShards []kr.ShardKey

	TargetKeyRange kr.KeyRange

	traceMsgs bool

	Qr      qrouter.Qrouter
	Cl      RouterClient
	manager ConnManager

	msgBuf []*pgproto3.Query
}

func NewRelayState(qr qrouter.Qrouter, client RouterClient, manager ConnManager) *RelayStateImpl {
	return &RelayStateImpl{
		ActiveShards: nil,
		TxActive:     false,
		msgBuf:       nil,
		traceMsgs:    false,
		Qr:           qr,
		Cl:           client,
		manager:      manager,
	}
}

func (rst *RelayStateImpl) Reset() error {
	rst.ActiveShards = nil
	rst.TxActive = false

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

func (rst *RelayStateImpl) Reroute(q *pgproto3.Query) ([]qrouter.ShardRoute, error) {
	span := opentracing.StartSpan("reroute to data shard")
	defer span.Finish()
	span.SetTag("user", rst.Cl.Usr())
	span.SetTag("db", rst.Cl.DB())
	span.SetTag("query", q.String)

	shardRoutes := rst.Qr.Route(q.String)
	span.SetTag("shard_routes", shardRoutes)
	tracelog.InfoLogger.Printf("parsed routes %v", shardRoutes)

	if len(shardRoutes) == 0 {
		tracelog.InfoLogger.PrintError(qrouter.MatchShardError)
		_ = rst.Cl.ReplyNotice(qrouter.MatchShardError.Error())
		return nil, qrouter.MatchShardError
	}

	if err := rst.manager.UnRouteCB(rst.Cl, rst.ActiveShards); err != nil {
		tracelog.ErrorLogger.PrintError(err)
	}

	rst.ActiveShards = nil
	for _, shr := range shardRoutes {
		rst.ActiveShards = append(rst.ActiveShards, shr.Shkey)
	}

	return shardRoutes, nil
}

func (rst *RelayStateImpl) RerouteWorld() ([]qrouter.ShardRoute, error) {
	span := opentracing.StartSpan("reroute to world")
	defer span.Finish()
	span.SetTag("user", rst.Cl.Usr())
	span.SetTag("db", rst.Cl.DB())

	shardRoutes := rst.Qr.WorldShardsRoutes()

	if len(shardRoutes) == 0 {
		tracelog.InfoLogger.PrintError(qrouter.MatchShardError)
		_ = rst.manager.UnRouteWithError(rst.Cl, nil, qrouter.MatchShardError)
		return nil, qrouter.MatchShardError
	}

	if err := rst.manager.UnRouteCB(rst.Cl, rst.ActiveShards); err != nil {
		tracelog.ErrorLogger.PrintError(err)
	}

	rst.ActiveShards = nil
	for _, shr := range shardRoutes {
		rst.ActiveShards = append(rst.ActiveShards, shr.Shkey)
	}

	return shardRoutes, nil
}

func (rst *RelayStateImpl) Connect(shardRoutes []qrouter.ShardRoute) error {

	var serv Server
	var err error

	if len(shardRoutes) > 1 {
		serv, err = NewMultiShardServer(rst.Cl.Route().beRule, rst.Cl.Route().servPool)
		if err != nil {
			return err
		}
	} else {
		tracelog.InfoLogger.Printf("initialize shard server conn")
		_ = rst.Cl.ReplyNotice("initialize single shard server conn")
		serv = NewShardServer(rst.Cl.Route().beRule, rst.Cl.Route().servPool)
	}

	if err := rst.Cl.AssignServerConn(serv); err != nil {
		return err
	}

	tracelog.InfoLogger.Printf("route cl %s:%s to %v", rst.Cl.Usr(), rst.Cl.DB(), shardRoutes)

	if err := rst.manager.RouteCB(rst.Cl, rst.ActiveShards); err != nil {
		tracelog.ErrorLogger.Printf("failed to route cl %w", err)
		return err
	}

	return nil
}

func (rst *RelayStateImpl) ConnectWold() error {

	tracelog.InfoLogger.Printf("initialize shard server conn")
	_ = rst.Cl.ReplyNotice("initialize single shard server conn")

	serv := NewShardServer(rst.Cl.Route().beRule, rst.Cl.Route().servPool)

	if err := rst.Cl.AssignServerConn(serv); err != nil {
		return err
	}

	tracelog.InfoLogger.Printf("route cl %s:%s to world shard", rst.Cl.Usr(), rst.Cl.DB())

	if err := rst.manager.RouteCB(rst.Cl, rst.ActiveShards); err != nil {
		tracelog.ErrorLogger.Printf("failed to route cl %w", err)
		return err
	}

	return nil
}

const TXREL = 73

func (rst *RelayStateImpl) RelayStep(v *pgproto3.Query) (byte, error) {

	if !rst.TxActive {

		if err := rst.manager.TXBeginCB(rst.Cl, rst); err != nil {
			return 0, err
		}
		rst.TxActive = true
	}

	if rst.traceMsgs {
		rst.msgBuf = append(rst.msgBuf, v)
	}

	var txst byte
	var err error
	if txst, err = rst.Cl.ProcQuery(v); err != nil {
		return 0, err
	}

	return txst, nil
}

func (rst *RelayStateImpl) ShouldRetry(err error) bool {
	return false
}

func (rst *RelayStateImpl) CompleteRelay(txst byte) error {

	tracelog.InfoLogger.Printf("complete relay iter with TX status %v", txst)

	if err := rst.Cl.Send(&pgproto3.ReadyForQuery{}); err != nil {
		return err
	}

	if txst == TXREL {
		if rst.TxActive {
			if err := rst.manager.TXEndCB(rst.Cl, rst); err != nil {
				return err
			}
			rst.TxActive = false
		}
	} else {
		if !rst.TxActive {
			if err := rst.manager.TXBeginCB(rst.Cl, rst); err != nil {
				return err
			}
			rst.TxActive = true
		}
	}

	return nil
}

func (rst *RelayStateImpl) ReplayBuff() pgproto3.FrontendMessage {
	var frmsg pgproto3.FrontendMessage

	for _, msg := range rst.msgBuf {
		_, _ = rst.Cl.ProcQuery(msg)
		frmsg, _ = rst.Cl.Receive()
	}

	return frmsg
}

func (rst *RelayStateImpl) UnRouteWithError(shkey []kr.ShardKey, errmsg error) error {

	_ = rst.manager.UnRouteWithError(rst.Cl, shkey, errmsg)

	return rst.Reset()
}

var _ RelayStateInteractor = &RelayStateImpl{}
