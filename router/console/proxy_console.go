package console

import (
	"context"
	"fmt"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/coord"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/rrelation"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/rfqn"
)

// Create reference relation on master coordinator. Be careful, it's a network-based operation.
//
// Parameters:
// - ctx: (context.Context): context
// - localCoordinator (meta.EntityMgr): Current (local) coordinator
// - clauseNode (*lyx.RangeVar): table definition
//
// Returns:
//   - error: An error when fails.
func CreateReferenceRelation(ctx context.Context, localMngr meta.EntityMgr, clauseNode *lyx.RangeVar) error {
	shs, err := localMngr.ListShards(ctx)
	if err != nil {
		return err
	}
	shardIds := []string{}
	for _, sh := range shs {
		shardIds = append(shardIds, sh.ID)
	}
	newReferenceRelation := &rrelation.ReferenceRelation{
		TableName:     clauseNode.RelationName,
		SchemaVersion: 1,
		ShardIds:      shardIds,
	}

	mgr, cf, err := coord.DistributedMgr(ctx, localMngr)
	if err != nil {
		return err
	}
	defer cf()

	err = mgr.CreateReferenceRelation(ctx, newReferenceRelation, nil)
	if err != nil {
		return err
	}
	return nil
}

func innerAlterDistributionAttach(ctx context.Context, mngr meta.EntityMgr, rv *lyx.RangeVar,
	distributionId string, distributionKey string) error {
	distribution, err := mngr.GetDistribution(ctx, distributionId)
	if err != nil {
		return err
	}
	if len(distribution.ColTypes) != 1 {
		return fmt.Errorf("automatic attach is supported only for distribution with one column key")
	}
	if distribution.ColTypes[0] != qdb.ColumnTypeVarchar &&
		distribution.ColTypes[0] != qdb.ColumnTypeInteger &&
		distribution.ColTypes[0] != qdb.ColumnTypeUUID {
		return fmt.Errorf("automatic attach isn't supported for column key %s", distribution.ColTypes[0])
	}
	attachRelation := []*distributions.DistributedRelation{
		{
			Relation:           rfqn.RelationFQNFromFullName(rv.SchemaName, rv.RelationName),
			ReplicatedRelation: false,
			DistributionKey: []distributions.DistributionKeyEntry{
				{
					Column: distributionKey,
					/* support hash function here */
				},
			},
		},
	}
	err = mngr.AlterDistributionAttach(ctx, distributionId, attachRelation)
	return err
}

// Attach relation to distribution on master coordinator. Be careful, it's a network-based operation.
//
// Parameters:
// - ctx: (context.Context): context
// - localCoordinator (meta.EntityMgr): Current (local) coordinator
// - clauseNode (*lyx.RangeVar): table definition
// - distributionId (string): distribution for attach
// - distributionKey (string): distribution key of attached relation
//
// Returns:
//   - error: An error when fails.
func AlterDistributionAttach(ctx context.Context, localMngr meta.EntityMgr, rv *lyx.RangeVar,
	distributionId string, distributionKey string) error {
	if distributionId == distributions.REPLICATED {
		return fmt.Errorf("can't attach distributed relation to REPLICATED distribution")
	}
	mgr, cf, err := coord.DistributedMgr(ctx, localMngr)
	if err != nil {
		return err
	}
	defer cf()

	return innerAlterDistributionAttach(ctx, mgr, rv,
		distributionId, distributionKey)
}
