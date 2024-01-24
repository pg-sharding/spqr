package coord

import (
	"context"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"

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

// TODO : unit tests
// TODO : implement
func (a *adapter) ShareKeyRange(id string) error {
	return spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "ShareKeyRange not implemented")
}

// TODO : unit tests
func (a *adapter) ListKeyRanges(ctx context.Context, dataspace string) ([]*kr.KeyRange, error) {
	c := proto.NewKeyRangeServiceClient(a.conn)
	reply, err := c.ListKeyRange(ctx, &proto.ListKeyRangeRequest{
		Dataspace: dataspace,
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
func (a *adapter) ListAllKeyRanges(ctx context.Context) ([]*kr.KeyRange, error) {
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
func (a *adapter) AddKeyRange(ctx context.Context, kr *kr.KeyRange) error {
	c := proto.NewKeyRangeServiceClient(a.conn)
	_, err := c.AddKeyRange(ctx, &proto.AddKeyRangeRequest{
		KeyRangeInfo: kr.ToProto(),
	})
	return err
}

// TODO : unit tests
func (a *adapter) LockKeyRange(ctx context.Context, krid string) (*kr.KeyRange, error) {
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

// TODO : unit tests
func (a *adapter) Split(ctx context.Context, split *kr.SplitKeyRange) error {
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
func (a *adapter) Unite(ctx context.Context, unite *kr.UniteKeyRange) error {
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
func (a *adapter) Move(ctx context.Context, move *kr.MoveKeyRange) error {
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
func (a *adapter) DropKeyRange(ctx context.Context, krid string) error {
	c := proto.NewKeyRangeServiceClient(a.conn)
	_, err := c.DropKeyRange(ctx, &proto.DropKeyRangeRequest{
		Id: []string{krid},
	})
	return err
}

// TODO : unit tests
func (a *adapter) DropKeyRangeAll(ctx context.Context) error {
	c := proto.NewKeyRangeServiceClient(a.conn)
	_, err := c.DropAllKeyRanges(ctx, &proto.DropAllKeyRangesRequest{})
	return err
}

// TODO : unit tests
func (a *adapter) AddShardingRule(ctx context.Context, rule *shrule.ShardingRule) error {
	c := proto.NewShardingRulesServiceClient(a.conn)
	_, err := c.AddShardingRules(ctx, &proto.AddShardingRuleRequest{
		Rules: []*proto.ShardingRule{shrule.ShardingRuleToProto(rule)},
	})
	return err
}

// TODO : unit tests
func (a *adapter) DropShardingRule(ctx context.Context, id string) error {
	c := proto.NewShardingRulesServiceClient(a.conn)
	_, err := c.DropShardingRules(ctx, &proto.DropShardingRuleRequest{
		Id: []string{id},
	})
	return err
}

// TODO : unit tests
func (a *adapter) DropShardingRuleAll(ctx context.Context) ([]*shrule.ShardingRule, error) {
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
func (a *adapter) ListShardingRules(ctx context.Context, dataspace string) ([]*shrule.ShardingRule, error) {
	c := proto.NewShardingRulesServiceClient(a.conn)
	reply, err := c.ListShardingRules(ctx, &proto.ListShardingRuleRequest{
		Dataspace: dataspace,
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
func (a *adapter) ListAllShardingRules(ctx context.Context) ([]*shrule.ShardingRule, error) {
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
func (a *adapter) RegisterRouter(ctx context.Context, r *topology.Router) error {
	c := proto.NewRouterServiceClient(a.conn)
	_, err := c.AddRouter(ctx, &proto.AddRouterRequest{
		Router: topology.RouterToProto(r),
	})
	return err
}

// TODO : unit tests
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

// TODO : unit tests
func (a *adapter) UnregisterRouter(ctx context.Context, id string) error {
	c := proto.NewRouterServiceClient(a.conn)
	_, err := c.RemoveRouter(ctx, &proto.RemoveRouterRequest{
		Id: id,
	})
	return err
}

// TODO : unit tests
func (a *adapter) SyncRouterMetadata(ctx context.Context, router *topology.Router) error {
	c := proto.NewRouterServiceClient(a.conn)
	_, err := c.SyncMetadata(ctx, &proto.SyncMetadataRequest{
		Router: topology.RouterToProto(router),
	})
	return err
}

// TODO : unit tests
// TODO : implement
func (a *adapter) AddDataShard(ctx context.Context, shard *datashards.DataShard) error {
	return spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "addDataShard not implemented")
}

// TODO : unit tests
// TODO : implement
func (a *adapter) AddWorldShard(ctx context.Context, shard *datashards.DataShard) error {
	return spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "addWorldShard not implemented")
}

// TODO : unit tests
func (a *adapter) ListShards(ctx context.Context) ([]*datashards.DataShard, error) {
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
func (a *adapter) GetShardInfo(ctx context.Context, shardID string) (*datashards.DataShard, error) {
	c := proto.NewShardServiceClient(a.conn)
	resp, err := c.GetShardInfo(ctx, &proto.ShardRequest{Id: shardID})
	return &datashards.DataShard{
		ID:  resp.ShardInfo.Id,
		Cfg: &config.Shard{Hosts: resp.ShardInfo.Hosts},
	}, err
}

// TODO : unit tests
func (a *adapter) ListDataspace(ctx context.Context) ([]*dataspaces.Dataspace, error) {
	c := proto.NewDataspaceServiceClient(a.conn)

	resp, err := c.ListDataspace(ctx, &proto.ListDataspaceRequest{})
	if err != nil {
		return nil, err
	}

	dss := make([]*dataspaces.Dataspace, len(resp.Dataspaces))
	for i, ds := range resp.Dataspaces {
		dss[i] = dataspaces.DataspaceFromProto(ds)
	}

	return dss, nil
}

// TODO : unit tests
func (a *adapter) AddDataspace(ctx context.Context, ds *dataspaces.Dataspace) error {
	c := proto.NewDataspaceServiceClient(a.conn)

	_, err := c.AddDataspace(ctx, &proto.AddDataspaceRequest{
		Dataspaces: []*proto.Dataspace{dataspaces.DataspaceToProto(ds)},
	})
	return err
}

// TODO : unit tests
func (a *adapter) DropDataspace(ctx context.Context, ds *dataspaces.Dataspace) error {
	c := proto.NewDataspaceServiceClient(a.conn)

	_, err := c.DropDataspace(ctx, &proto.DropDataspaceRequest{
		Ids: []string{ds.Id},
	})

	return err
}

// TODO : unit tests
func (a *adapter) AttachToDataspace(ctx context.Context, table string, ds *dataspaces.Dataspace) error {
	c := proto.NewDataspaceServiceClient(a.conn)

	_, err := c.AttachToDataspace(ctx, &proto.AttachToDataspaceRequest{
		Table:     table,
		Dataspace: dataspaces.DataspaceToProto(ds),
	})

	return err
}

// TODO : unit tests
func (a *adapter) GetDataspace(ctx context.Context, table string) (*dataspaces.Dataspace, error) {
	c := proto.NewDataspaceServiceClient(a.conn)

	resp, err := c.GetDataspace(ctx, &proto.GetDataspaceRequest{Table: table})
	if err != nil {
		return nil, err
	}

	return dataspaces.DataspaceFromProto(resp.Dataspace), nil
}

// TODO : unit tests
func (a *adapter) UpdateCoordinator(ctx context.Context, address string) error {
	c := proto.NewTopologyServiceClient(a.conn)
	_, err := c.UpdateCoordinator(ctx, &proto.UpdateCoordinatorRequest{Address: address})
	return err
}

// TODO : unit tests
func (a *adapter) GetCoordinator(ctx context.Context) (string, error) {
	c := proto.NewTopologyServiceClient(a.conn)
	resp, err := c.GetCoordinator(ctx, &proto.GetCoordinatorRequest{})
	return resp.Address, err
}
