package coord

import (
	"context"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"

	"github.com/pg-sharding/spqr/pkg/models/datashards"
	"github.com/pg-sharding/spqr/pkg/models/keyspaces"
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
	return spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "shareKeyRange not implemented")
}

// TODO : unit tests
func (a *adapter) ListKeyRanges(ctx context.Context, keyspace string) ([]*kr.KeyRange, error) {
	c := proto.NewKeyRangeServiceClient(a.conn)
	reply, err := c.ListKeyRange(ctx, &proto.ListKeyRangeRequest{
		Keyspace: keyspace,
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
						UpperBound: string(keyRange.UpperBound),
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

	if left == nil || right == nil {
		return spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, "key range on left or right was not found")
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
func (a *adapter) ListShardingRules(ctx context.Context, keyspace string) ([]*shrule.ShardingRule, error) {
	c := proto.NewShardingRulesServiceClient(a.conn)
	reply, err := c.ListShardingRules(ctx, &proto.ListShardingRuleRequest{
		Keyspace: keyspace,
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
// TODO : implement
func (a *adapter) ListShards(ctx context.Context) ([]*datashards.DataShard, error) {
	return nil, spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "ListShards not implemented")
}

// TODO : unit tests
// TODO : implement
func (a *adapter) GetShardInfo(ctx context.Context, shardID string) (*datashards.DataShard, error) {
	return nil, spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "GetShardInfo not implemented")
}

// TODO : unit tests
func (a *adapter) ListKeyspace(ctx context.Context) ([]*keyspaces.Keyspace, error) {
	c := proto.NewKeyspaceServiceClient(a.conn)

	resp, err := c.ListKeyspace(ctx, &proto.ListKeyspaceRequest{})
	if err != nil {
		return nil, err
	}

	dss := make([]*keyspaces.Keyspace, len(resp.Keyspaces))
	for i, ds := range resp.Keyspaces {
		dss[i] = keyspaces.KeyspaceFromProto(ds)
	}

	return dss, nil
}

// TODO : unit tests
func (a *adapter) AddKeyspace(ctx context.Context, ds *keyspaces.Keyspace) error {
	c := proto.NewKeyspaceServiceClient(a.conn)

	_, err := c.AddKeyspace(ctx, &proto.AddKeyspaceRequest{
		Keyspaces: []*proto.Keyspace{keyspaces.KeyspaceToProto(ds)},
	})
	return err
}

// TODO : unit tests
func (a *adapter) DropKeyspace(ctx context.Context, ds *keyspaces.Keyspace) error {
	c := proto.NewKeyspaceServiceClient(a.conn)

	_, err := c.DropKeyspace(ctx, &proto.DropKeyspaceRequest{
		Ids: []string{ds.Id},
	})

	return err
}

// TODO : unit tests
func (a *adapter) AlterKeyspaceAttachRelation(ctx context.Context, table string, ds *keyspaces.Keyspace) error {
	c := proto.NewKeyspaceServiceClient(a.conn)

	_, err := c.AlterKeyspaceAttachRelation(ctx, &proto.AlterKeyspaceAttachRelationRequest{
		
		Relations: ,
		Table:     table,
	})

	return err
}

// TODO : unit tests
func (a *adapter) GetKeyspace(ctx context.Context, table string) (*keyspaces.Keyspace, error) {
	c := proto.NewKeyspaceServiceClient(a.conn)

	resp, err := c.GetKeyspaceForRelation(ctx, &proto.GetKeyspaceForRelationRequest{
		RelationName: table,
	})
	if err != nil {
		return nil, err
	}

	return keyspaces.KeyspaceFromProto(resp.Keyspace), nil
}

// TODO : unit tests
// TODO : implement
func (a *adapter) UpdateCoordinator(ctx context.Context, address string) error {
	return spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "UpdateCoordinator not implemeneted")
}

// TODO : unit tests
// TODO : implement
func (a *adapter) GetCoordinator(ctx context.Context) (string, error) {
	return "", spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "GetCoordinator not implemented")
}
