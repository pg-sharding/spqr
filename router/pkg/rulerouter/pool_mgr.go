package rulerouter

import (
	"fmt"
	"sort"
	"strings"

	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/pkg/spqrlog"

	"github.com/jackc/pgproto3/v2"
	"github.com/pkg/errors"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/router/pkg/client"
)

type PoolMgr interface {
	TXBeginCB(rst RelayStateMgr) error
	TXEndCB(rst RelayStateMgr) error

	RouteCB(client client.RouterClient, sh []kr.ShardKey) error
	UnRouteCB(client client.RouterClient, sh []kr.ShardKey) error
	UnRouteWithError(client client.RouterClient, sh []kr.ShardKey, errmsg error) error

	ValidateReRoute(rst RelayStateMgr) bool
	ConnectionActive(rst RelayStateMgr) bool
}

func unRouteWithError(cmngr PoolMgr, client client.RouterClient, sh []kr.ShardKey, errmsg error) error {
	_ = cmngr.UnRouteCB(client, sh)
	return client.ReplyErrMsg(errmsg.Error())
}

type TxConnManager struct {
	ReplyNotice bool
}

func (t *TxConnManager) UnRouteWithError(client client.RouterClient, sh []kr.ShardKey, errmsg error) error {
	return unRouteWithError(t, client, sh, errmsg)
}

func (t *TxConnManager) UnRouteCB(cl client.RouterClient, sh []kr.ShardKey) error {
	for _, shkey := range sh {
		spqrlog.Logger.Printf(spqrlog.DEBUG1, "client %p unrouting from datashard %v", cl, shkey.Name)
		if err := cl.Server().UnRouteShard(shkey, cl.Rule()); err != nil {
			_ = cl.Unroute()
			return err
		}
	}
	return cl.Unroute()
}

func NewTxConnManager(rcfg *config.Router) *TxConnManager {
	return &TxConnManager{
		ReplyNotice: rcfg.ShowNoticeMessages,
	}
}

func replyShardMatches(client client.RouterClient, sh []kr.ShardKey) error {
	var shardNames []string
	for _, shkey := range sh {
		shardNames = append(shardNames, shkey.Name)
	}
	sort.Strings(shardNames)
	shardMatches := strings.Join(shardNames, ",")

	return client.ReplyNotice("send query to shard(s) : " + shardMatches)
}

func (t *TxConnManager) RouteCB(client client.RouterClient, sh []kr.ShardKey) error {
	if t.ReplyNotice {
		if err := replyShardMatches(client, sh); err != nil {
			return err
		}
	}

	for _, shkey := range sh {
		if err := client.Server().AddDataShard(shkey); err != nil {
			return err
		}
	}

	return nil
}

func (t *TxConnManager) ConnectionActive(rst RelayStateMgr) bool {
	return rst.ActiveShards() != nil
}

func (t *TxConnManager) ValidateReRoute(rst RelayStateMgr) bool {
	return rst.ActiveShards() == nil || rst.TxStatus() == conn.TXIDLE
}

func (t *TxConnManager) TXBeginCB(rst RelayStateMgr) error {
	return nil
}

func (t *TxConnManager) TXEndCB(rst RelayStateMgr) error {
	ash := rst.ActiveShards()
	spqrlog.Logger.Printf(spqrlog.LOG, "end of tx unrouting from %v", ash)
	rst.ActiveShardsReset()

	if err := t.UnRouteCB(rst.Client(), ash); err != nil {
		return err
	}

	return nil
}

type SessConnManager struct {
	ReplyNotice bool
}

func (s *SessConnManager) UnRouteWithError(client client.RouterClient, sh []kr.ShardKey, errmsg error) error {
	return unRouteWithError(s, client, sh, errmsg)
}

func (s *SessConnManager) UnRouteCB(cl client.RouterClient, sh []kr.ShardKey) error {
	for _, shkey := range sh {
		if err := cl.Server().UnRouteShard(shkey, cl.Rule()); err != nil {
			return err
		}
	}

	return nil
}

func (s *SessConnManager) TXBeginCB(rst RelayStateMgr) error {
	return nil
}

func (s *SessConnManager) TXEndCB(rst RelayStateMgr) error {
	return nil
}

func (s *SessConnManager) RouteCB(client client.RouterClient, sh []kr.ShardKey) error {
	if s.ReplyNotice {
		if err := replyShardMatches(client, sh); err != nil {
			return err
		}
	}

	for _, shkey := range sh {
		if err := client.Server().AddDataShard(shkey); err != nil {
			return err
		}
	}

	return nil
}

func (t *SessConnManager) ConnectionActive(rst RelayStateMgr) bool {
	return rst.ActiveShards() != nil
}

func (s *SessConnManager) ValidateReRoute(rst RelayStateMgr) bool {
	return rst.ActiveShards() == nil
}

func NewSessConnManager(rcfg *config.Router) *SessConnManager {
	return &SessConnManager{
		ReplyNotice: rcfg.ShowNoticeMessages,
	}
}

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
