package qrouter

import (
	"context"
	"fmt"
	"math/rand"
	"sync"

	"go.uber.org/atomic"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/datashards"
	"github.com/pg-sharding/spqr/pkg/models/dataspaces"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/shrule"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/qdb/ops"
)

type ProxyQrouter struct {
	mu sync.Mutex

	Rules []*shrule.ShardingRule

	ColumnMapping map[string]struct{}
	LocalTables   map[string]struct{}

	// shards
	DataShardCfgs  map[string]*config.Shard
	WorldShardCfgs map[string]*config.Shard

	cfg *config.QRouter

	initialized *atomic.Bool

	qdb qdb.QDB
}

var _ QueryRouter = &ProxyQrouter{}

func (qr *ProxyQrouter) ListDataspace(ctx context.Context) ([]*dataspaces.Dataspace, error) {
	qr.mu.Lock()
	defer qr.mu.Unlock()

	resp, err := qr.qdb.ListDataspaces(ctx)
	if err != nil {
		return nil, err
	}
	var retDsp []*dataspaces.Dataspace

	for _, dsp := range resp {
		retDsp = append(retDsp, &dataspaces.Dataspace{
			Id: dsp.ID,
		})
	}
	return retDsp, nil
}

func (qr *ProxyQrouter) AddDataspace(ctx context.Context, ds *dataspaces.Dataspace) error {
	qr.mu.Lock()
	defer qr.mu.Unlock()
	return qr.qdb.AddDataspace(ctx, &qdb.Dataspace{
		ID: ds.Id,
	})
}

func (qr *ProxyQrouter) Initialized() bool {
	return qr.initialized.Load()
}

func (qr *ProxyQrouter) Initialize() bool {
	return qr.initialized.Swap(true)
}

func (qr *ProxyQrouter) ListDataShards(ctx context.Context) []*datashards.DataShard {
	qr.mu.Lock()
	defer qr.mu.Unlock()

	var ret []*datashards.DataShard
	for id, cfg := range qr.DataShardCfgs {
		ret = append(ret, datashards.NewDataShard(id, cfg))
	}
	return ret
}

func (qr *ProxyQrouter) ListShards(ctx context.Context) ([]*datashards.DataShard, error) {
	resp, err := qr.qdb.ListShards(ctx)
	if err != nil {
		return nil, err
	}
	var retShards []*datashards.DataShard

	for _, sh := range resp {
		retShards = append(retShards, &datashards.DataShard{
			ID: sh.ID,
			Cfg: &config.Shard{
				Hosts: sh.Hosts,
			},
		})
	}
	return retShards, nil
}

func (qr *ProxyQrouter) AddWorldShard(ctx context.Context, ds *datashards.DataShard) error {
	qr.mu.Lock()
	defer qr.mu.Unlock()

	spqrlog.Logger.Printf(spqrlog.LOG, "adding world datashard %s", ds.ID)
	qr.WorldShardCfgs[ds.ID] = ds.Cfg

	return nil
}

func (qr *ProxyQrouter) DropKeyRange(ctx context.Context, id string) error {
	qr.mu.Lock()
	defer qr.mu.Unlock()

	spqrlog.Logger.Printf(spqrlog.LOG, "dropping key range %s", id)
	return qr.qdb.DropKeyRange(ctx, id)
}

func (qr *ProxyQrouter) DropKeyRangeAll(ctx context.Context) error {
	qr.mu.Lock()
	defer qr.mu.Unlock()

	spqrlog.Logger.Printf(spqrlog.LOG, "dropping all key range")
	return qr.qdb.DropKeyRangeAll(ctx)
}

func (qr *ProxyQrouter) DataShardsRoutes() []*DataShardRoute {
	qr.mu.Lock()
	defer qr.mu.Unlock()

	var ret []*DataShardRoute

	for name := range qr.DataShardCfgs {
		ret = append(ret, &DataShardRoute{
			Shkey: kr.ShardKey{
				Name: name,
				RW:   true,
			},
		})
	}

	return ret
}

func (qr *ProxyQrouter) WorldShardsRoutes() []*DataShardRoute {
	qr.mu.Lock()
	defer qr.mu.Unlock()

	var ret []*DataShardRoute

	for name := range qr.WorldShardCfgs {
		ret = append(ret, &DataShardRoute{
			Shkey: kr.ShardKey{
				Name: name,
				RW:   true,
			},
		})
	}

	// a sort of round robin

	rand.Shuffle(len(ret), func(i, j int) {
		ret[i], ret[j] = ret[j], ret[i]
	})
	return ret
}

func (qr *ProxyQrouter) WorldShards() []string {
	qr.mu.Lock()
	defer qr.mu.Unlock()

	var ret []string

	for name := range qr.WorldShardCfgs {
		ret = append(ret, name)
	}

	return ret
}

func NewProxyRouter(shardMapping map[string]*config.Shard, qcfg *config.QRouter) (*ProxyQrouter, error) {
	db, err := qdb.NewMemQDB()
	if err != nil {
		return nil, err
	}

	proxy := &ProxyQrouter{
		DataShardCfgs:  map[string]*config.Shard{},
		WorldShardCfgs: map[string]*config.Shard{},
		qdb:            db,
		initialized:    atomic.NewBool(false),
		cfg:            qcfg,
	}

	for name, shardCfg := range shardMapping {
		switch shardCfg.Type {
		case config.WorldShard:
		case config.DataShard:
			fallthrough // default is datashard
		default:
			if err := proxy.AddDataShard(context.TODO(), &datashards.DataShard{
				ID:  name,
				Cfg: shardCfg,
			}); err != nil {
				return nil, err
			}
		}
	}
	return proxy, nil
}

func (qr *ProxyQrouter) Move(ctx context.Context, req *kr.MoveKeyRange) error {
	var krmv *qdb.KeyRange
	var err error
	if krmv, err = qr.qdb.CheckLockedKeyRange(ctx, req.Krid); err != nil {
		return err
	}

	var reqKr = kr.KeyRangeFromDB(krmv)
	reqKr.ShardID = req.ShardId
	return ops.ModifyKeyRangeWithChecks(ctx, qr.qdb, reqKr)
}

func (qr *ProxyQrouter) Unite(ctx context.Context, req *kr.UniteKeyRange) error {

	var krleft *qdb.KeyRange
	var err error

	if krleft, err = qr.qdb.LockKeyRange(ctx, req.KeyRangeIDLeft); err != nil {
		return err
	}
	defer func(qdb qdb.QDB, ctx context.Context, keyRangeID string) {
		err := qdb.UnlockKeyRange(ctx, keyRangeID)
		if err != nil {
			spqrlog.Logger.PrintError(err)
			return
		}
	}(qr.qdb, ctx, req.KeyRangeIDLeft)

	// TODO: krRight seems to be empty.
	if krleft, err = qr.qdb.LockKeyRange(ctx, req.KeyRangeIDRight); err != nil {
		return err
	}
	defer func(qdb qdb.QDB, ctx context.Context, keyRangeID string) {
		err := qdb.UnlockKeyRange(ctx, keyRangeID)
		if err != nil {
			spqrlog.Logger.PrintError(err)
			return
		}
	}(qr.qdb, ctx, req.KeyRangeIDRight)

	if err = qr.qdb.DropKeyRange(ctx, krleft.KeyRangeID); err != nil {
		return err
	}

	var krRight *kr.KeyRange
	krRight.LowerBound = krleft.LowerBound

	return ops.ModifyKeyRangeWithChecks(ctx, qr.qdb, krRight)
}

func (qr *ProxyQrouter) Split(ctx context.Context, req *kr.SplitKeyRange) error {
	var krOld *qdb.KeyRange
	var err error

	if krOld, err = qr.qdb.LockKeyRange(ctx, req.SourceID); err != nil {
		return err
	}
	defer func(qdb qdb.QDB, ctx context.Context, krid string) {
		err := qdb.UnlockKeyRange(ctx, krid)
		if err != nil {
			spqrlog.Logger.PrintError(err)
		}
	}(qr.qdb, ctx, req.SourceID)

	krNew := kr.KeyRangeFromDB(
		&qdb.KeyRange{
			LowerBound: req.Bound,
			UpperBound: krOld.UpperBound,
			KeyRangeID: req.SourceID,
		},
	)

	if err := ops.AddKeyRangeWithChecks(ctx, qr.qdb, krNew); err != nil {
		return err
	}
	krOld.UpperBound = req.Bound
	_ = qr.qdb.UpdateKeyRange(ctx, krOld)

	return nil
}

func (qr *ProxyQrouter) LockKeyRange(ctx context.Context, krid string) (*kr.KeyRange, error) {
	keyRangeDB, err := qr.qdb.LockKeyRange(ctx, krid)
	if err != nil {
		return nil, err
	}

	return kr.KeyRangeFromDB(keyRangeDB), nil
}

func (qr *ProxyQrouter) Unlock(ctx context.Context, krid string) error {
	return qr.qdb.UnlockKeyRange(ctx, krid)
}

func (qr *ProxyQrouter) AddDataShard(ctx context.Context, ds *datashards.DataShard) error {
	spqrlog.Logger.Printf(spqrlog.LOG, "adding node %s", ds.ID)
	qr.DataShardCfgs[ds.ID] = ds.Cfg

	return qr.qdb.AddShard(ctx, &qdb.Shard{
		ID:    ds.ID,
		Hosts: ds.Cfg.Hosts,
	})
}

func (qr *ProxyQrouter) Shards() []string {
	var ret []string

	for name := range qr.DataShardCfgs {
		ret = append(ret, name)
	}

	return ret
}

func (qr *ProxyQrouter) ListKeyRanges(ctx context.Context) ([]*kr.KeyRange, error) {
	var ret []*kr.KeyRange
	if krs, err := qr.qdb.ListKeyRanges(ctx); err != nil {
		return nil, err
	} else {
		for _, keyRange := range krs {
			ret = append(ret, kr.KeyRangeFromDB(keyRange))
		}
	}

	return ret, nil
}

func (qr *ProxyQrouter) ListRouters(ctx context.Context) ([]*topology.Router, error) {
	return []*topology.Router{{
		Id: "local",
	}}, nil
}

func (qr *ProxyQrouter) AddShardingRule(ctx context.Context, rule *shrule.ShardingRule) error {
	return ops.AddShardingRuleWithChecks(ctx, qr.qdb, rule)
}

func (qr *ProxyQrouter) ListShardingRules(ctx context.Context) ([]*shrule.ShardingRule, error) {
	rules, err := qr.qdb.ListShardingRules(ctx)
	if err != nil {
		return nil, err
	}
	var resp []*shrule.ShardingRule
	for _, v := range rules {
		resp = append(resp, shrule.ShardingRuleFromDB(v))
	}

	return resp, nil
}

func (qr *ProxyQrouter) DropShardingRule(ctx context.Context, id string) error {
	return qr.qdb.DropShardingRule(ctx, id)
}

func (qr *ProxyQrouter) AddKeyRange(ctx context.Context, kr *kr.KeyRange) error {
	return ops.AddKeyRangeWithChecks(ctx, qr.qdb, kr)
}

func (qr *ProxyQrouter) MoveKeyRange(ctx context.Context, kr *kr.KeyRange) error {
	return ops.ModifyKeyRangeWithChecks(ctx, qr.qdb, kr)
}

var ErrNotCoordinator = fmt.Errorf("request is unprocessable in route")

func (qr *ProxyQrouter) DropShardingRuleAll(ctx context.Context) ([]*shrule.ShardingRule, error) {
	rules, err := qr.qdb.DropShardingRuleAll(ctx)
	if err != nil {
		return nil, err
	}
	var retRules []*shrule.ShardingRule

	for _, r := range rules {
		retRules = append(retRules, shrule.ShardingRuleFromDB(r))
	}

	return retRules, nil
}

func (qr *ProxyQrouter) RegisterRouter(ctx context.Context, r *topology.Router) error {
	return ErrNotCoordinator
}

func (qr *ProxyQrouter) UnregisterRouter(ctx context.Context, id string) error {
	return ErrNotCoordinator
}

func (qr *ProxyQrouter) SyncRouterMetadata(ctx context.Context, router *topology.Router) error {
	return ErrNotCoordinator
}

func (qr *ProxyQrouter) GetShardInfo(ctx context.Context, shardID string) (*datashards.DataShard, error) {
	return nil, ErrNotCoordinator
}

func (qr *ProxyQrouter) Subscribe(krid string, keyRangeStatus *qdb.KeyRangeStatus, noitfyio chan<- interface{}) error {
	return nil
}
