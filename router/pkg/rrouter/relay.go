package rrouter

import (
	"fmt"
	"github.com/pg-sharding/spqr/router/pkg/parser"

	"github.com/jackc/pgproto3/v2"
	"github.com/opentracing/opentracing-go"
	"github.com/wal-g/tracelog"
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
	Parse(q pgproto3.Query) error
	AddQuery(q pgproto3.FrontendMessage)
	ActiveShards() []kr.ShardKey
	ActiveShardsReset()
	TxActive() bool
	RelayStep(waitForResp bool) (byte, error)
	UnRouteWithError(shkey []kr.ShardKey, errmsg error) error
	CompleteRelay(txst byte) error
	Close() error
}

type RelayStateImpl struct {
	txActive   bool
	sync       int
	CopyActive bool

	activeShards   []kr.ShardKey
	TargetKeyRange kr.KeyRange

	traceMsgs bool

	Qr      qrouter.QueryRouter
	Cl      client.RouterClient
	manager ConnManager

	msgBuf []pgproto3.FrontendMessage
	parser parser.QParser
}

func NewRelayState(qr qrouter.QueryRouter, client client.RouterClient, manager ConnManager) RelayStateInteractor {
	return &RelayStateImpl{
		activeShards: nil,
		txActive:     false,
		msgBuf:       nil,
		traceMsgs:    false,
		Qr:           qr,
		Cl:           client,
		manager:      manager,
	}
}

func (rst *RelayStateImpl) Close() error {
	if err := rst.manager.UnRouteCB(rst.Cl, rst.activeShards); err != client.NotRouted {
		tracelog.ErrorLogger.PrintError(err)
		return err
	}
	return rst.Cl.Close()
}

func (rst *RelayStateImpl) TxActive() bool {
	return rst.txActive
}

func (rst *RelayStateImpl) ActiveShardsReset() {
	rst.activeShards = nil
}

func (rst *RelayStateImpl) ActiveShards() []kr.ShardKey {
	return rst.activeShards
}

func (rst *RelayStateImpl) Reset() error {
	rst.activeShards = nil
	rst.txActive = false

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

func (rst *RelayStateImpl) Reroute() error {
	tracelog.InfoLogger.Printf("rerouting")
	_ = rst.Cl.ReplyNotice(fmt.Sprintf("rerouting your connection"))

	span := opentracing.StartSpan("reroute")
	defer span.Finish()
	span.SetTag("user", rst.Cl.Usr())
	span.SetTag("db", rst.Cl.DB())
	span.SetTag("query", rst.parser.Q().String)

	routingState, err := rst.Qr.Route(rst.parser.Q().String)
	_ = rst.Cl.ReplyNotice(fmt.Sprintf("rerouting state %T %v", routingState, err))
	if err != nil {
		return err
	}

	switch v := routingState.(type) {
	case qrouter.ShardMatchState:
		tracelog.InfoLogger.Printf("parsed routes %v", routingState)
		//
		if len(v.Routes) == 0 {
			tracelog.InfoLogger.PrintError(qrouter.MatchShardError)
			return qrouter.MatchShardError
		}

		if err := rst.manager.UnRouteCB(rst.Cl, rst.activeShards); err != client.NotRouted {
			tracelog.ErrorLogger.PrintError(err)
			return err
		}

		rst.activeShards = nil
		for _, shr := range v.Routes {
			rst.activeShards = append(rst.activeShards, shr.Shkey)
		}
		//
		if err := rst.Cl.ReplyNotice(fmt.Sprintf("matched datashard routes %+v", v.Routes)); err != nil {
			return err
		}

		if err := rst.Connect(v.Routes); err != nil {
			tracelog.InfoLogger.Printf("encounter %w while initialing server connection", err)
			_ = rst.Reset()
			_ = rst.Cl.ReplyErrMsg(err.Error())
			return err
		}

		return nil
	case qrouter.SkipRoutingState:
		return SkipQueryError
	case qrouter.WolrdRouteState:
		if !config.RouterConfig().RulesConfig.WorldShardFallback {
			return err
		}

		// fallback to execute query on wolrd datashard (s)
		_, _ = rst.RerouteWorld()
		if err := rst.ConnectWorld(); err != nil {
			_ = rst.UnRouteWithError(nil, xerrors.Errorf("failed to fallback on world datashard: %w", err))
			return err
		}

		return nil
	default:
		tracelog.ErrorLogger.PrintError(xerrors.Errorf("unexpected route state %T", v))
		return xerrors.Errorf("unexpected route state %T", v)
	}

	//span.SetTag("shard_routes", routingState)
}

func (rst *RelayStateImpl) RerouteWorld() ([]*qrouter.ShardRoute, error) {
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

	if err := rst.manager.UnRouteCB(rst.Cl, rst.activeShards); err != nil {
		tracelog.ErrorLogger.PrintError(err)
	}

	rst.activeShards = nil
	for _, shr := range shardRoutes {
		rst.activeShards = append(rst.activeShards, shr.Shkey)
	}

	return shardRoutes, nil
}

func (rst *RelayStateImpl) Connect(shardRoutes []*qrouter.ShardRoute) error {
	var serv server.Server
	var err error

	if len(shardRoutes) > 1 {
		serv, err = server.NewMultiShardServer(rst.Cl.Route().BeRule(), rst.Cl.Route().ServPool())
		if err != nil {
			return err
		}
	} else {
		tracelog.InfoLogger.Printf("initialize datashard server conn")
		_ = rst.Cl.ReplyNotice("initialize single datashard server conn")
		serv = server.NewShardServer(rst.Cl.Route().BeRule(), rst.Cl.Route().ServPool())
	}

	if err := rst.Cl.AssignServerConn(serv); err != nil {
		return err
	}

	tracelog.InfoLogger.Printf("route cl %s:%s to %v", rst.Cl.Usr(), rst.Cl.DB(), shardRoutes)

	if err := rst.manager.RouteCB(rst.Cl, rst.activeShards); err != nil {
		tracelog.ErrorLogger.Printf("failed to route cl %w", err)
		return err
	}

	return nil
}

func (rst *RelayStateImpl) ConnectWorld() error {
	tracelog.InfoLogger.Printf("initialize datashard server conn")
	_ = rst.Cl.ReplyNotice("initialize single datashard server conn")

	serv := server.NewShardServer(rst.Cl.Route().BeRule(), rst.Cl.Route().ServPool())

	if err := rst.Cl.AssignServerConn(serv); err != nil {
		return err
	}

	tracelog.InfoLogger.Printf("route cl %s:%s to world datashard", rst.Cl.Usr(), rst.Cl.DB())

	if err := rst.manager.RouteCB(rst.Cl, rst.activeShards); err != nil {
		tracelog.ErrorLogger.Printf("failed to route cl %w", err)
		return err
	}

	return nil
}

func (rst *RelayStateImpl) RelayCopyStep(msg *pgproto3.FrontendMessage) error {
	return rst.Cl.ProcCopy(msg)
}

func (rst *RelayStateImpl) RelayCopyComplete(msg *pgproto3.FrontendMessage) error {
	rst.CopyActive = false
	return rst.Cl.ProcCopyComplete(msg)
}

func (rst *RelayStateImpl) RelayStep(waitForResp bool) (byte, error) {
	if !rst.txActive {
		if err := rst.manager.TXBeginCB(rst.Cl, rst); err != nil {
			return 0, err
		}
		rst.txActive = true
	}

	var txst byte
	var err error

	for len(rst.msgBuf) > 0 {
		var v pgproto3.FrontendMessage
		v, rst.msgBuf = rst.msgBuf[0], rst.msgBuf[1:]
		if txst, err = rst.Cl.ProcQuery(v, waitForResp); err != nil {
			return 0, err
		}
	}

	return txst, nil
}

func (rst *RelayStateImpl) ShouldRetry(err error) bool {
	return false
}

func (rst *RelayStateImpl) CompleteRelay(txst byte) error {
	tracelog.InfoLogger.Printf("complete relay iter with TX status %v", txst)

	if !rst.CopyActive {
		switch txst {
		case conn.TXIDLE:
			if err := rst.Cl.Send(&pgproto3.ReadyForQuery{
				TxStatus: txst,
			}); err != nil {
				return err
			}

			if rst.txActive {
				if err := rst.manager.TXEndCB(rst.Cl, rst); err != nil {
					return err
				}
				rst.txActive = false
			}
			return nil
		case conn.TXERR:
			fallthrough
		case conn.TXACT:
			if err := rst.Cl.Send(&pgproto3.ReadyForQuery{
				TxStatus: txst,
			}); err != nil {
				return err
			}
			if !rst.txActive {
				if err := rst.manager.TXBeginCB(rst.Cl, rst); err != nil {
					return err
				}
				rst.txActive = true
			}
			return nil
		case conn.TXCONT:
			return nil
		default:
			err := xerrors.Errorf("unknown tx status %v", txst)
			tracelog.ErrorLogger.PrintError(err)
			return err
		}
	}
	return nil
}

func (rst *RelayStateImpl) UnRouteWithError(shkey []kr.ShardKey, errmsg error) error {
	_ = rst.manager.UnRouteWithError(rst.Cl, shkey, errmsg)
	return rst.Reset()
}

func (rst *RelayStateImpl) AddQuery(q pgproto3.FrontendMessage) {
	rst.msgBuf = append(rst.msgBuf, q)
}

func (rst *RelayStateImpl) Parse(q pgproto3.Query) error {
	return rst.parser.Parse(q)
}

var _ RelayStateInteractor = &RelayStateImpl{}
