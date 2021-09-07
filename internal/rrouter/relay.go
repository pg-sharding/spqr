package rrouter

import (
	"github.com/jackc/pgproto3"
	"github.com/pg-sharding/spqr/internal/qdb"
	"github.com/pg-sharding/spqr/internal/qrouter"
	"github.com/wal-g/tracelog"
	"golang.org/x/xerrors"
)

type RelayState struct {
	TxActive bool

	ActiveShards []qdb.ShardKey
}

func (rst *RelayState) Reroute(rt qrouter.Qrouter, cl Client, cmngr ConnManager, q *pgproto3.Query) error {

	shards := rt.Route(q.String)

	if len(shards) == 0 {
		_ = cmngr.UnRouteWithError(cl, nil, "failed to match shard")
		return xerrors.New("failed to match shard")
	}

	if err := cmngr.UnRouteCB(cl, rst.ActiveShards); err != nil {
		tracelog.ErrorLogger.PrintError(err)
	}
	rst.ActiveShards = shards

	var serv Server
	var err error

	if len(shards) > 1 {
		serv, err = NewMultiShardServer(cl.Route().beRule, cl.Route().servPool)
		if err != nil {
			return err
		}
	} else {
		serv = NewShardServer(cl.Route().beRule, cl.Route().servPool)
	}

	if err := cl.AssignServerConn(serv); err != nil {
		return err
	}

	tracelog.InfoLogger.Printf("route cl %s:%s to %v", cl.Usr(), cl.DB(), shards)

	if err := cmngr.RouteCB(cl, rst.ActiveShards); err != nil {
		tracelog.ErrorLogger.PrintError(err)
	}

	return nil
}

const TXREL = 73

func (rst *RelayState) RelayStep(cl Client, cmngr ConnManager) error {

	if !rst.TxActive {
		if err := cmngr.TXBeginCB(cl, rst); err != nil {
			return err
		}
		rst.TxActive = true
	}

	return nil
}

func (rst *RelayState) CompleteRelay(cl Client, cmngr ConnManager, txst byte) error {

	tracelog.InfoLogger.Printf("complete relay iter with TX status %v", txst)

	if err := cl.Send(&pgproto3.ReadyForQuery{}); err != nil {
		return err
	}

	if txst == TXREL {
		if rst.TxActive {
			if err := cmngr.TXEndCB(cl, rst); err != nil {
				return err
			}
			rst.TxActive = false
		}
	} else {
		if !rst.TxActive {
			if err := cmngr.TXBeginCB(cl, rst); err != nil {
				return err
			}
			rst.TxActive = true
		}
	}

	return nil
}
