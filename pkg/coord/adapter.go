package coord

import (
	"context"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"

	"github.com/pg-sharding/spqr/pkg/models/datashards"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/shrule"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
	"google.golang.org/grpc"
)

type Adapter struct {
	conn *grpc.ClientConn
}

var _ meta.EntityMgr = &Adapter{}

func NewAdapter(conn *grpc.ClientConn) *Adapter {
	return &Adapter{
		conn: conn,
	}
}

func (a *Adapter) QDB() qdb.QDB {
	return nil
}

// TODO : unit tests
// TODO : implement
func (a *Adapter) ShareKeyRange(id string) error {
	return spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "ShareKeyRange not implemented")
}

// TODO : unit tests
func (a *Adapter) ListKeyRanges(ctx context.Context, distribution string) ([]*kr.KeyRange, error) {
	c := proto.NewKeyRangeServiceClient(a.conn)
	reply, err := c.ListKeyRange(ctx, &proto.ListKeyRangeRequest{
		Distribution: distribution,
	})
	if err != nil {
		return nil, err
	}

	krs := make([]*kr.KeyRange, len(reply.KeyRangesInfo))
	for i, keyRange := range reply.KeyRangesInfo {
		krs[i] = kr.KeyRangeFromProto(keyRange)
	}

	return krs, nil
}

// TODO : unit tests
func (a *Adapter) ListAllKeyRanges(ctx context.Context) ([]*kr.KeyRange, error) {
	c := proto.NewKeyRangeServiceClient(a.conn)
	reply, err := c.ListKeyRange(ctx, &proto.ListKeyRangeRequest{})
	if err != nil {
		return nil, err
	}

	krs := make([]*kr.KeyRange, len(reply.KeyRangesInfo))
	for i, keyRange := range reply.KeyRangesInfo {
		krs[i] = kr.KeyRangeFromProto(keyRange)
	}

	return krs, nil
}

// TODO : unit tests
func (a *Adapter) AddKeyRange(ctx context.Context, kr *kr.KeyRange) error {
	c := proto.NewKeyRangeServiceClient(a.conn)
	_, err := c.AddKeyRange(ctx, &proto.AddKeyRangeRequest{
		KeyRangeInfo: kr.ToProto(),
	})
	return err
}

// TODO : unit tests
func (a *Adapter) LockKeyRange(ctx context.Context, krid string) (*kr.KeyRange, error) {
	c := proto.NewKeyRangeServiceClient(a.conn)
	_, err := c.LockKeyRange(ctx, &proto.LockKeyRangeRequest{
		Id: []string{krid},
	})
	if err != nil {
		return nil, err
	}

	krs, err := a.ListAllKeyRanges(ctx)
	if err != nil {
		return nil, err
	}

	for _, kr := range krs {
		if kr.ID == krid {
			return kr, nil
		}
	}

	return nil, spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range with id %s not found", krid)
}

// TODO : unit tests
func (a *Adapter) UnlockKeyRange(ctx context.Context, krid string) error {
	c := proto.NewKeyRangeServiceClient(a.conn)
	_, err := c.UnlockKeyRange(ctx, &proto.UnlockKeyRangeRequest{
		Id: []string{krid},
	})
	if err != nil {
		return err
	}

	return nil
}

// TODO : unit tests
func (a *Adapter) Split(ctx context.Context, split *kr.SplitKeyRange) error {
	krs, err := a.ListAllKeyRanges(ctx)
	if err != nil {
		return err
	}

	for _, keyRange := range krs {
		if keyRange.ID == split.SourceID {
			c := proto.NewKeyRangeServiceClient(a.conn)
			_, err := c.SplitKeyRange(ctx, &proto.SplitKeyRangeRequest{
				Bound:    split.Bound,
				SourceId: split.SourceID,
				KeyRangeInfo: &proto.KeyRangeInfo{
					Krid:    split.Krid,
					ShardId: keyRange.ShardID,
					KeyRange: &proto.KeyRange{
						LowerBound: string(keyRange.LowerBound),
					},
				},
			})
			return err
		}
	}

	return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range with id %s not found", split.Krid)
}

// TODO : unit tests
func (a *Adapter) Unite(ctx context.Context, unite *kr.UniteKeyRange) error {
	krs, err := a.ListAllKeyRanges(ctx)
	if err != nil {
		return err
	}

	var left *kr.KeyRange
	var right *kr.KeyRange

	for _, kr := range krs {
		if kr.ID == unite.KeyRangeIDLeft {
			left = kr
		}
		if kr.ID == unite.KeyRangeIDRight {
			right = kr
		}
	}

	for _, krcurr := range krs {
		if krcurr.ID == unite.KeyRangeIDLeft || krcurr.ID == unite.KeyRangeIDRight {
			continue
		}
		if kr.CmpRangesLess(krcurr.LowerBound, right.LowerBound) && kr.CmpRangesLess(left.LowerBound, krcurr.LowerBound) {
			return spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, "unvalid unite request")
		}
	}

	if left == nil || right == nil || kr.CmpRangesLess(right.LowerBound, left.LowerBound) {
		return spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, "key range on left or right was not found")
	}

	c := proto.NewKeyRangeServiceClient(a.conn)
	_, err = c.MergeKeyRange(ctx, &proto.MergeKeyRangeRequest{
		Bound: right.LowerBound,
	})
	return err
}

// TODO : unit tests
func (a *Adapter) Move(ctx context.Context, move *kr.MoveKeyRange) error {
	krs, err := a.ListAllKeyRanges(ctx)
	if err != nil {
		return err
	}

	for _, keyRange := range krs {
		if keyRange.ID == move.Krid {
			c := proto.NewKeyRangeServiceClient(a.conn)
			_, err := c.MoveKeyRange(ctx, &proto.MoveKeyRangeRequest{
				KeyRange:  keyRange.ToProto(),
				ToShardId: move.ShardId,
			})
			return err
		}
	}

	return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range with id %s not found", move.Krid)
}

// TODO : unit tests
func (a *Adapter) DropKeyRange(ctx context.Context, krid string) error {
	c := proto.NewKeyRangeServiceClient(a.conn)
	_, err := c.DropKeyRange(ctx, &proto.DropKeyRangeRequest{
		Id: []string{krid},
	})
	return err
}

// TODO : unit tests
func (a *Adapter) DropKeyRangeAll(ctx context.Context) error {
	c := proto.NewKeyRangeServiceClient(a.conn)
	_, err := c.DropAllKeyRanges(ctx, &proto.DropAllKeyRangesRequest{})
	return err
}

// TODO : unit tests
func (a *Adapter) AddShardingRule(ctx context.Context, rule *shrule.ShardingRule) error {
	c := proto.NewShardingRulesServiceClient(a.conn)
	_, err := c.AddShardingRules(ctx, &proto.AddShardingRuleRequest{
		Rules: []*proto.ShardingRule{shrule.ShardingRuleToProto(rule)},
	})
	return err
}

// TODO : unit tests
func (a *Adapter) DropShardingRule(ctx context.Context, id string) error {
	c := proto.NewShardingRulesServiceClient(a.conn)
	_, err := c.DropShardingRules(ctx, &proto.DropShardingRuleRequest{
		Id: []string{id},
	})
	return err
}

// TODO : unit tests
func (a *Adapter) DropShardingRuleAll(ctx context.Context) ([]*shrule.ShardingRule, error) {
	rules, err := a.ListAllShardingRules(ctx)
	if err != nil {
		return nil, err
	}
	ids := make([]string, len(rules))
	for i, rule := range rules {
		ids[i] = rule.Id
	}

	c := proto.NewShardingRulesServiceClient(a.conn)
	_, err = c.DropShardingRules(ctx, &proto.DropShardingRuleRequest{Id: ids})
	return rules, err
}

// TODO : unit tests
func (a *Adapter) ListShardingRules(ctx context.Context, distribution string) ([]*shrule.ShardingRule, error) {
	c := proto.NewShardingRulesServiceClient(a.conn)
	reply, err := c.ListShardingRules(ctx, &proto.ListShardingRuleRequest{
		Distribution: distribution,
	})
	if err != nil {
		return nil, err
	}

	shrules := make([]*shrule.ShardingRule, len(reply.Rules))
	for i, sh := range reply.Rules {
		shrules[i] = shrule.ShardingRuleFromProto(sh)
	}

	return shrules, nil
}

// TODO : unit tests
func (a *Adapter) ListAllShardingRules(ctx context.Context) ([]*shrule.ShardingRule, error) {
	c := proto.NewShardingRulesServiceClient(a.conn)
	reply, err := c.ListShardingRules(ctx, &proto.ListShardingRuleRequest{})
	if err != nil {
		return nil, err
	}

	shrules := make([]*shrule.ShardingRule, len(reply.Rules))
	for i, sh := range reply.Rules {
		shrules[i] = shrule.ShardingRuleFromProto(sh)
	}

	return shrules, nil
}

// TODO : unit tests
func (a *Adapter) RegisterRouter(ctx context.Context, r *topology.Router) error {
	c := proto.NewRouterServiceClient(a.conn)
	_, err := c.AddRouter(ctx, &proto.AddRouterRequest{
		Router: topology.RouterToProto(r),
	})
	return err
}

// TODO : unit tests
func (a *Adapter) ListRouters(ctx context.Context) ([]*topology.Router, error) {
	c := proto.NewRouterServiceClient(a.conn)
	resp, err := c.ListRouters(ctx, &proto.ListRoutersRequest{})
	if err != nil {
		return nil, err
	}
	routers := []*topology.Router{}
	for _, r := range resp.Routers {
		routers = append(routers, topology.RouterFromProto(r))
	}
	return routers, nil
}

// TODO : unit tests
func (a *Adapter) UnregisterRouter(ctx context.Context, id string) error {
	c := proto.NewRouterServiceClient(a.conn)
	_, err := c.RemoveRouter(ctx, &proto.RemoveRouterRequest{
		Id: id,
	})
	return err
}

// TODO : unit tests
func (a *Adapter) SyncRouterMetadata(ctx context.Context, router *topology.Router) error {
	c := proto.NewRouterServiceClient(a.conn)
	_, err := c.SyncMetadata(ctx, &proto.SyncMetadataRequest{
		Router: topology.RouterToProto(router),
	})
	return err
}

// TODO : unit tests
// TODO : implement
func (a *Adapter) AddDataShard(ctx context.Context, shard *datashards.DataShard) error {
	return spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "addDataShard not implemented")
}

// TODO : unit tests
// TODO : implement
func (a *Adapter) AddWorldShard(ctx context.Context, shard *datashards.DataShard) error {
	return spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "addWorldShard not implemented")
}

// TODO : unit tests
func (a *Adapter) ListShards(ctx context.Context) ([]*datashards.DataShard, error) {
	c := proto.NewShardServiceClient(a.conn)
	resp, err := c.ListShards(ctx, &proto.ListShardsRequest{})
	shards := resp.Shards
	var ds []*datashards.DataShard
	for _, shard := range shards {
		ds = append(ds, &datashards.DataShard{
			ID:  shard.Id,
			Cfg: &config.Shard{Hosts: shard.Hosts},
		})
	}
	return ds, err
}

// TODO : unit tests
func (a *Adapter) GetShardInfo(ctx context.Context, shardID string) (*datashards.DataShard, error) {
	c := proto.NewShardServiceClient(a.conn)
	resp, err := c.GetShardInfo(ctx, &proto.ShardRequest{Id: shardID})
	return &datashards.DataShard{
		ID:  resp.ShardInfo.Id,
		Cfg: &config.Shard{Hosts: resp.ShardInfo.Hosts},
	}, err
}

// TODO : unit tests
func (a *Adapter) ListDistribution(ctx context.Context) ([]*distributions.Distribution, error) {
	c := proto.NewDistributionServiceClient(a.conn)

	resp, err := c.ListDistribution(ctx, &proto.ListDistributionRequest{})
	if err != nil {
		return nil, err
	}

	dss := make([]*distributions.Distribution, len(resp.Distributions))
	for i, ds := range resp.Distributions {
		dss[i] = distributions.DistributionFromProto(ds)
	}

	return dss, nil
}

// TODO : unit tests
func (a *Adapter) CreateDistribution(ctx context.Context, ds *distributions.Distribution) error {
	c := proto.NewDistributionServiceClient(a.conn)

	_, err := c.CreateDistribution(ctx, &proto.CreateDistributionRequest{
		Distributions: []*proto.Distribution{
			distributions.DistributionToProto(ds),
		},
	})
	return err
}

// TODO : unit tests
func (a *Adapter) DropDistribution(ctx context.Context, id string) error {
	c := proto.NewDistributionServiceClient(a.conn)

	_, err := c.DropDistribution(ctx, &proto.DropDistributionRequest{
		Ids: []string{id},
	})

	return err
}

// TODO : unit tests
func (a *Adapter) AlterDistributionAttach(ctx context.Context, id string, rels []*distributions.DistributedRelation) error {
	c := proto.NewDistributionServiceClient(a.conn)

	dRels := []*proto.DistributedRelation{}
	for _, r := range rels {
		dRels = append(dRels, distributions.DistributedRelatitonToProto(r))
	}

	_, err := c.AlterDistributionAttach(ctx, &proto.AlterDistributionAttachRequest{
		Id:        id,
		Relations: dRels,
	})

	return err
}

// TODO : unit tests
func (a *Adapter) GetDistribution(ctx context.Context, id string) (*distributions.Distribution, error) {
	c := proto.NewDistributionServiceClient(a.conn)

	resp, err := c.GetDistribution(ctx, &proto.GetDistributionRequest{
		Id: id,
	})
	if err != nil {
		return nil, err
	}

	return distributions.DistributionFromProto(resp.Distribution), nil
}

func (a *Adapter) GetRelationDistribution(ctx context.Context, id string) (*distributions.Distribution, error) {
	c := proto.NewDistributionServiceClient(a.conn)

	resp, err := c.GetRelationDistribution(ctx, &proto.GetRelationDistributionRequest{
		Id: id,
	})
	if err != nil {
		return nil, err
	}

	return distributions.DistributionFromProto(resp.Distribution), nil
}

// TODO : unit tests
func (a *Adapter) UpdateCoordinator(ctx context.Context, address string) error {
	c := proto.NewTopologyServiceClient(a.conn)
	_, err := c.UpdateCoordinator(ctx, &proto.UpdateCoordinatorRequest{Address: address})
	return err
}

// TODO : unit tests
func (a *Adapter) GetCoordinator(ctx context.Context) (string, error) {
	c := proto.NewTopologyServiceClient(a.conn)
	resp, err := c.GetCoordinator(ctx, &proto.GetCoordinatorRequest{})
	return resp.Address, err
}
