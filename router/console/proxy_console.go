package console

import (
	"context"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/rrelation"
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

	mgr, cf, err := distributedMgr(ctx, localMngr)
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
