package internal

import (
	"github.com/jackc/pgproto3"
	"github.com/pg-sharding/spqr/internal/qrouter"
	"github.com/wal-g/tracelog"
)

type RelayState struct {
	TxActive bool

	ActiveShards []Shard
}

func (rst *RelayState) reroute(rt qrouter.Qrouter, cl Client, cmngr ConnManager, q *pgproto3.Query) error {

	shardNames := rt.Route(q.String)

	var shards []Shard

	for _, name := range shardNames {
		shards = append(shards, NewShard(name, rt.ShardCfg(name)))
	}

	if shardNames == nil {
		return cmngr.UnRouteWithError(cl, nil, "failed to match shard")
	} else {
		tracelog.InfoLogger.Printf("parsed shard name %s", shardNames)
	}

	if rst.ActiveShards != nil {

		for _, shard := range rst.ActiveShards {
			if err := cmngr.UnRouteCB(cl, shard); err != nil {
				tracelog.ErrorLogger.PrintError(err)
			}
		}
	}

	rst.ActiveShards = shards

	for _, shard := range rst.ActiveShards {
		if err := cmngr.RouteCB(cl, shard); err != nil {
			tracelog.ErrorLogger.PrintError(err)
		}
	}

	return nil
}

const TXREL = 73

func (rst *RelayState) completeRelay(cl Client, cmngr ConnManager, txst byte) error {

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
