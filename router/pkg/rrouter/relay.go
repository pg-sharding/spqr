package rrouter

import (
	"github.com/jackc/pgproto3"
	"github.com/opentracing/opentracing-go"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/router/pkg/qrouter"
	"github.com/wal-g/tracelog"
	"golang.org/x/xerrors"
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
	cl      Client
	manager ConnManager

	msgBuf []*pgproto3.Query
}

func NewRelayState(qr qrouter.Qrouter, client Client, manager ConnManager) *RelayStateImpl {
	return &RelayStateImpl{
		ActiveShards: nil,
		TxActive:     false,
		msgBuf:       nil,
		traceMsgs:    false,
		Qr:           qr,
		cl:           client,
		manager:      manager,
	}
}

func (rst *RelayStateImpl) Reset() error {
	rst.ActiveShards = nil
	rst.TxActive = false

	rst.cl.Server().Reset()
	return nil
}

func (rst *RelayStateImpl) StartTrace() {
	rst.traceMsgs = true
}

func (rst *RelayStateImpl) Flush() {
	rst.msgBuf = nil
	rst.traceMsgs = false
}

func (rst *RelayStateImpl) Reroute(q *pgproto3.Query) ([]qrouter.ShardRoute, error) {
	span := opentracing.StartSpan("reroute")
	defer span.Finish()
	span.SetTag("user", rst.cl.Usr())
	span.SetTag("db", rst.cl.DB())
	span.SetTag("query", q.String)

	shardRoutes := rst.Qr.Route(q.String)
	span.SetTag("shard_routes", shardRoutes)
	tracelog.InfoLogger.Printf("parsed routes %v", shardRoutes)

	if len(shardRoutes) == 0 {
		tracelog.InfoLogger.Printf("failed to match shard")
		_ = rst.manager.UnRouteWithError(rst.cl, nil, "failed to match shard")
		return nil, xerrors.New("failed to match shard")
	}

	if err := rst.manager.UnRouteCB(rst.cl, rst.ActiveShards); err != nil {
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
		serv, err = NewMultiShardServer(rst.cl.Route().beRule, rst.cl.Route().servPool)
		if err != nil {
			return err
		}
	} else {
		tracelog.InfoLogger.Printf("initialize shard server conn")
		_ = rst.cl.ReplyNotice("initialize single shard server conn")
		serv = NewShardServer(rst.cl.Route().beRule, rst.cl.Route().servPool)
	}

	if err := rst.cl.AssignServerConn(serv); err != nil {
		return err
	}

	tracelog.InfoLogger.Printf("route cl %s:%s to %v", rst.cl.Usr(), rst.cl.DB(), shardRoutes)

	if err := rst.manager.RouteCB(rst.cl, rst.ActiveShards); err != nil {
		tracelog.ErrorLogger.Printf("failed to route cl %w", err)
		return err
	}

	return nil
}

const TXREL = 73

func (rst *RelayStateImpl) RelayStep(v *pgproto3.Query) (byte, error) {

	if !rst.TxActive {

		if err := rst.manager.TXBeginCB(rst.cl, rst); err != nil {
			return 0, err
		}
		rst.TxActive = true
	}

	if rst.traceMsgs {
		rst.msgBuf = append(rst.msgBuf, v)
	}

	var txst byte
	var err error
	if txst, err = rst.cl.ProcQuery(v); err != nil {
		return 0, err
	}

	return txst, nil
}

func (rst *RelayStateImpl) ShouldRetry(err error) bool {
	return false
}

func (rst *RelayStateImpl) CompleteRelay(txst byte) error {

	tracelog.InfoLogger.Printf("complete relay iter with TX status %v", txst)

	if err := rst.cl.Send(&pgproto3.ReadyForQuery{}); err != nil {
		return err
	}

	if txst == TXREL {
		if rst.TxActive {
			if err := rst.manager.TXEndCB(rst.cl, rst); err != nil {
				return err
			}
			rst.TxActive = false
		}
	} else {
		if !rst.TxActive {
			if err := rst.manager.TXBeginCB(rst.cl, rst); err != nil {
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
		_, _ = rst.cl.ProcQuery(msg)
		frmsg, _ = rst.cl.Receive()
	}

	return frmsg
}

var _ RelayStateInteractor = &RelayStateImpl{}