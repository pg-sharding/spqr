package coord

import (
	"context"
	"fmt"

	"github.com/pg-sharding/spqr/pkg/models/datashards"
	"github.com/pg-sharding/spqr/pkg/models/dataspaces"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/shrule"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
	"google.golang.org/grpc"
)

type adapter struct {
	conn *grpc.ClientConn
}

func NewAdapter(conn *grpc.ClientConn) *adapter {
	return &adapter{
		conn: conn,
	}
}

func (a *adapter) QDB() qdb.QDB {
	return nil
}

func (a *adapter) ShareKeyRange(id string) error {
	return fmt.Errorf("shareKeyRange not implemented")
}

func (a *adapter) ListKeyRanges(ctx context.Context, _ string) ([]*kr.KeyRange, error) {
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

func (a *adapter) AddKeyRange(ctx context.Context, kr *kr.KeyRange) error {
	c := proto.NewKeyRangeServiceClient(a.conn)
	_, err := c.AddKeyRange(ctx, &proto.AddKeyRangeRequest{
		KeyRangeInfo: kr.ToProto(),
	})
	return err
}

func (a *adapter) LockKeyRange(ctx context.Context, krid string) (*kr.KeyRange, error) {
	c := proto.NewKeyRangeServiceClient(a.conn)
	_, err := c.LockKeyRange(ctx, &proto.LockKeyRangeRequest{
		Id: []string{krid},
	})
	if err != nil {
		return nil, err
	}

	krs, err := a.ListKeyRanges(ctx, "")
	if err != nil {
		return nil, err
	}

	for _, kr := range krs {
		if kr.ID == krid {
			return kr, nil
		}
	}

	return nil, fmt.Errorf("key range with id %s not found", krid)
}

func (a *adapter) UnlockKeyRange(ctx context.Context, krid string) error {
	c := proto.NewKeyRangeServiceClient(a.conn)
	_, err := c.UnlockKeyRange(ctx, &proto.UnlockKeyRangeRequest{
		Id: []string{krid},
	})
	if err != nil {
		return err
	}

	return nil
}

func (a *adapter) Split(ctx context.Context, split *kr.SplitKeyRange) error {
	krs, err := a.ListKeyRanges(ctx, "")
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
						UpperBound: string(keyRange.UpperBound),
					},
				},
			})
			return err
		}
	}

	return fmt.Errorf("key range with id %s not found", split.Krid)
}

func (a *adapter) Unite(ctx context.Context, unite *kr.UniteKeyRange) error {
	krs, err := a.ListKeyRanges(ctx, "")
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

	if left == nil || right == nil {
		return fmt.Errorf("key range on left or right was not found")
	}

	var bound []byte
	if kr.CmpRangesEqual(left.UpperBound, right.LowerBound) {
		bound = left.UpperBound
	}
	if kr.CmpRangesEqual(left.LowerBound, right.UpperBound) {
		bound = left.LowerBound
	}

	c := proto.NewKeyRangeServiceClient(a.conn)
	_, err = c.MergeKeyRange(ctx, &proto.MergeKeyRangeRequest{
		Bound: bound,
	})
	return err
}

func (a *adapter) Move(ctx context.Context, move *kr.MoveKeyRange) error {
	krs, err := a.ListKeyRanges(ctx, "")
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

	return fmt.Errorf("key range with id %s not found", move.Krid)
}

func (a *adapter) DropKeyRange(ctx context.Context, krid string) error {
	c := proto.NewKeyRangeServiceClient(a.conn)
	_, err := c.DropKeyRange(ctx, &proto.DropKeyRangeRequest{
		Id: []string{krid},
	})
	return err
}

func (a *adapter) DropKeyRangeAll(ctx context.Context) error {
	c := proto.NewKeyRangeServiceClient(a.conn)
	_, err := c.DropAllKeyRanges(ctx, &proto.DropAllKeyRangesRequest{})
	return err
}

func (a *adapter) AddShardingRule(ctx context.Context, rule *shrule.ShardingRule) error {
	c := proto.NewShardingRulesServiceClient(a.conn)
	_, err := c.AddShardingRules(ctx, &proto.AddShardingRuleRequest{
		Rules: []*proto.ShardingRule{shrule.ShardingRuleToProto(rule)},
	})
	return err
}

func (a *adapter) DropShardingRule(ctx context.Context, id string) error {
	c := proto.NewShardingRulesServiceClient(a.conn)
	_, err := c.DropShardingRules(ctx, &proto.DropShardingRuleRequest{
		Id: []string{id},
	})
	return err
}

func (a *adapter) DropShardingRuleAll(ctx context.Context) ([]*shrule.ShardingRule, error) {
	rules, err := a.ListShardingRules(ctx, "")
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

func (a *adapter) ListShardingRules(ctx context.Context, _ string) ([]*shrule.ShardingRule, error) {
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

func (a *adapter) RegisterRouter(ctx context.Context, r *topology.Router) error {
	c := proto.NewRouterServiceClient(a.conn)
	_, err := c.AddRouter(ctx, &proto.AddRouterRequest{
		Router: topology.RouterToProto(r),
	})
	return err
}

func (a *adapter) ListRouters(ctx context.Context) ([]*topology.Router, error) {
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

func (a *adapter) UnregisterRouter(ctx context.Context, id string) error {
	c := proto.NewRouterServiceClient(a.conn)
	_, err := c.RemoveRouter(ctx, &proto.RemoveRouterRequest{
		Id: id,
	})
	return err
}

func (a *adapter) SyncRouterMetadata(ctx context.Context, router *topology.Router) error {
	c := proto.NewRouterServiceClient(a.conn)
	_, err := c.SyncMetadata(ctx, &proto.SyncMetadataRequest{
		Router: topology.RouterToProto(router),
	})
	return err
}

func (a *adapter) AddDataShard(ctx context.Context, shard *datashards.DataShard) error {
	return fmt.Errorf("addDataShard not implemented")
}

func (a *adapter) AddWorldShard(ctx context.Context, shard *datashards.DataShard) error {
	return fmt.Errorf("addWorldShard not implemented")
}

func (a *adapter) ListShards(ctx context.Context) ([]*datashards.DataShard, error) {
	return nil, fmt.Errorf("ListShards not implemented")
}

func (a *adapter) GetShardInfo(ctx context.Context, shardID string) (*datashards.DataShard, error) {
	return nil, fmt.Errorf("GetShardInfo not implemented")
}

func (a *adapter) ListDataspace(ctx context.Context) ([]*dataspaces.Dataspace, error) {
	return nil, fmt.Errorf("ListDataspace not implemented")
}

func (a *adapter) AddDataspace(ctx context.Context, ks *dataspaces.Dataspace) error {
	return fmt.Errorf("addDataspace not implemented")
}

func (a *adapter) UpdateCoordinator(ctx context.Context, address string) error {
	return fmt.Errorf("UpdateCoordinator not implemeneted")
}

func (a *adapter) GetCoordinator(ctx context.Context) (string, error) {
	return "", fmt.Errorf("GetCoordinator not implemented")
}
