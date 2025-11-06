package rmeta

import (
	"context"
	"fmt"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/coord"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/rrelation"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Returns master coordinator (Adapter to remote coordinator) with support of local coordinator.
// Returns local coordinator in case router in non clustered mode.
//
// Parameters:
// - ctx: (context.Context): context
// - localCoordinator (meta.EntityMgr): Current (local) coordinator
//
// Returns:
// - masterMngr (meta.EntityMgr) master coordinator
// - close (func() error) function for closing master coordinator connection.
// - error: An error when fails.
func getMasterCoordinator(ctx context.Context,
	localCoordinator meta.EntityMgr) (masterMngr meta.EntityMgr, close func() error, err error) {
	noNeedToClose := func() error { return nil }

	coordAddr, err := localCoordinator.GetCoordinator(ctx)
	if err != nil {
		return nil, noNeedToClose, err
	}
	if coordAddr == "" {
		return localCoordinator, noNeedToClose, nil
	}
	masterCoordinatorConn, err := grpc.NewClient(coordAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, noNeedToClose, err
	}
	masterCoordinator := coord.NewAdapter(masterCoordinatorConn)
	return masterCoordinator, func() error {
		return masterCoordinatorConn.Close()
	}, nil
}

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
	masterCoordinator, close, err := getMasterCoordinator(ctx, localMngr)
	defer func() {
		if err := close(); err != nil {
			spqrlog.Zero.Debug().Err(err).Msg("failed to close master coordinator connection (case 0)")
		}
	}()
	if err != nil {
		return err
	}
	newReferenceRelation := &rrelation.ReferenceRelation{
		TableName:     clauseNode.RelationName,
		SchemaVersion: 1,
		ShardIds:      shardIds,
	}
	err = masterCoordinator.CreateReferenceRelation(ctx, newReferenceRelation, nil)
	if err != nil {
		return err
	}
	return nil
}

func innerAlterDistributionAttach(ctx context.Context, mngr meta.EntityMgr, clauseNode *lyx.RangeVar,
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
			Name:               clauseNode.RelationName,
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
func AlterDistributionAttach(ctx context.Context, localMngr meta.EntityMgr, clauseNode *lyx.RangeVar,
	distributionId string, distributionKey string) error {
	if distributionId == distributions.REPLICATED {
		return fmt.Errorf("can't attach distributed relation to REPLICATED distribution")
	}
	masterCoordinator, close, err := getMasterCoordinator(ctx, localMngr)
	defer func() {
		if err := close(); err != nil {
			spqrlog.Zero.Debug().Err(err).Msg("failed to close master coordinator connection (case 1)")
		}
	}()
	if err != nil {
		return err
	}
	return innerAlterDistributionAttach(ctx, masterCoordinator, clauseNode,
		distributionId, distributionKey)
}
