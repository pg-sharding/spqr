package poolmgr

import (
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/pg-sharding/spqr/router/client"
	"github.com/pkg/errors"
)

type ConnectionKeeper interface {
	txstatus.TxStatusMgr
	ActiveShards() []kr.ShardKey
	ActiveShardsReset()
	Client() client.RouterClient
}

type PoolMgr interface {
	TXBeginCB(rst ConnectionKeeper) error
	TXEndCB(rst ConnectionKeeper) error

	RouteCB(client client.RouterClient, sh []kr.ShardKey) error
	UnRouteCB(client client.RouterClient, sh []kr.ShardKey) error
	UnRouteWithError(client client.RouterClient, sh []kr.ShardKey, errmsg error) error

	ValidateReRoute(rst ConnectionKeeper) bool
	ConnectionActive(rst ConnectionKeeper) bool
}

// TODO : unit tests
func unRouteWithError(cmngr PoolMgr, client client.RouterClient, sh []kr.ShardKey, errmsg error) error {
	_ = cmngr.UnRouteCB(client, sh)
	return client.ReplyErr(errmsg)
}

type TxConnManager struct {
	ReplyNotice bool
}

// TODO : unit tests
func (t *TxConnManager) UnRouteWithError(client client.RouterClient, sh []kr.ShardKey, errmsg error) error {
	return unRouteWithError(t, client, sh, errmsg)
}

var unsyncConnection = fmt.Errorf("failed to unroute client from connection with active TX")

// TODO : unit tests
func (t *TxConnManager) UnRouteCB(cl client.RouterClient, sh []kr.ShardKey) error {
	var anyerr error
	anyerr = nil

	cl.ServerAcquireUse()

	if cl.Server() == nil {
		/* If there is nothing to unroute, return */
		cl.ServerReleaseUse()
		return nil
	}

	if cl.Server().TxStatus() != txstatus.TXIDLE {
		cl.ServerReleaseUse()
		if err := cl.Server().Reset(); err != nil {
			return err
		}
		return unsyncConnection
	}

	for _, shkey := range sh {
		spqrlog.Zero.Debug().
			Uint("client", cl.ID()).
			Uint("shardn", spqrlog.GetPointer(cl.Server())).
			Str("key", shkey.Name).
			Msg("client unrouting from datashard")
		if err := cl.Server().UnRouteShard(shkey, cl.Rule()); err != nil {
			anyerr = err
		}
	}

	cl.ServerReleaseUse()
	_ = cl.Unroute()

	return anyerr
}

func NewTxConnManager(rcfg *config.Router) *TxConnManager {
	return &TxConnManager{
		ReplyNotice: rcfg.ShowNoticeMessages,
	}
}

// TODO : unit tests
func replyShardMatches(client client.RouterClient, sh []kr.ShardKey) error {
	var shardNames []string
	for _, shkey := range sh {
		shardNames = append(shardNames, shkey.Name)
	}
	sort.Strings(shardNames)
	shardMatches := strings.Join(shardNames, ",")

	return client.ReplyNotice("send query to shard(s) : " + shardMatches)
}

// TODO : unit tests
func (t *TxConnManager) RouteCB(client client.RouterClient, sh []kr.ShardKey) error {
	if t.ReplyNotice {
		if err := replyShardMatches(client, sh); err != nil {
			return err
		}
	}

	client.ServerAcquireUse()
	defer client.ServerReleaseUse()

	for _, shkey := range sh {
		spqrlog.Zero.Debug().
			Str("client tsa", client.GetTsa()).
			Msg("adding shard with tsa")
		if err := client.Server().AddDataShard(client.ID(), shkey, client.GetTsa()); err != nil {
			return err
		}
	}

	return nil
}

// TODO : unit tests
func (t *TxConnManager) ConnectionActive(rst ConnectionKeeper) bool {
	return rst.ActiveShards() != nil
}

// TODO : unit tests
func (t *TxConnManager) ValidateReRoute(rst ConnectionKeeper) bool {
	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Int("shards", len(rst.ActiveShards())).
		Msg("client validate rerouting of TX")

	return rst.ActiveShards() == nil || rst.TxStatus() == txstatus.TXIDLE
}

func (t *TxConnManager) TXBeginCB(rst ConnectionKeeper) error {
	return nil
}

// TODO : unit tests
func (t *TxConnManager) TXEndCB(rst ConnectionKeeper) error {
	ash := rst.ActiveShards()
	spqrlog.Zero.Debug().
		Uint("client", rst.Client().ID()).
		Msg("client end of transaction, unrouting from active shards")
	rst.ActiveShardsReset()

	return t.UnRouteCB(rst.Client(), ash)
}

type SessConnManager struct {
	ReplyNotice bool
}

// TODO : unit tests
func (s *SessConnManager) UnRouteWithError(client client.RouterClient, sh []kr.ShardKey, errmsg error) error {
	return unRouteWithError(s, client, sh, errmsg)
}

// TODO : unit tests
func (s *SessConnManager) UnRouteCB(cl client.RouterClient, sh []kr.ShardKey) error {
	var anyerr error
	anyerr = nil

	cl.ServerAcquireUse()
	defer cl.ServerReleaseUse()

	for _, shkey := range sh {
		if err := cl.Server().UnRouteShard(shkey, cl.Rule()); err != nil {
			//
			anyerr = err
		}
	}

	return anyerr
}

func (s *SessConnManager) TXBeginCB(rst ConnectionKeeper) error {
	return nil
}

func (s *SessConnManager) TXEndCB(rst ConnectionKeeper) error {
	return nil
}

// TODO : unit tests
func (s *SessConnManager) RouteCB(client client.RouterClient, sh []kr.ShardKey) error {
	if s.ReplyNotice {
		if err := replyShardMatches(client, sh); err != nil {
			return err
		}
	}

	client.ServerAcquireUse()
	defer client.ServerReleaseUse()

	for _, shkey := range sh {
		if err := client.Server().AddDataShard(client.ID(), shkey, client.GetTsa()); err != nil {
			return err
		}
	}

	return nil
}

// TODO : unit tests
func (t *SessConnManager) ConnectionActive(rst ConnectionKeeper) bool {
	return rst.ActiveShards() != nil
}

// TODO : unit tests
func (s *SessConnManager) ValidateReRoute(rst ConnectionKeeper) bool {
	return rst.ActiveShards() == nil
}

func NewSessConnManager(rcfg *config.Router) *SessConnManager {
	return &SessConnManager{
		ReplyNotice: rcfg.ShowNoticeMessages,
	}
}

// TODO : unit tests
func MatchConnectionPooler(client client.RouterClient, rcfg *config.Router) (PoolMgr, error) {
	switch client.Rule().PoolMode {
	case config.PoolModeSession:
		return NewSessConnManager(rcfg), nil
	case config.PoolModeTransaction:
		return NewTxConnManager(rcfg), nil
	default:
		for _, msg := range []pgproto3.BackendMessage{
			&pgproto3.ErrorResponse{
				Message:  fmt.Sprintf("unknown pool mode for route %v", client.ID()),
				Severity: "ERROR",
			},
		} {
			if err := client.Send(msg); err != nil {
				return nil, err
			}
		}

		return nil, errors.Errorf("unknown pool mode %v", client.Rule().PoolMode)
	}
}
