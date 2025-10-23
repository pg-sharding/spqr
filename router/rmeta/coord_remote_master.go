package rmeta

import (
	"context"
	"errors"
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

var ErrNoRemoteCoordinator = errors.New("remote master coordinator not found")

func getMasterCoordinatorConn(ctx context.Context, localCoordinator meta.EntityMgr) (*grpc.ClientConn, error) {
	coordAddr, err := localCoordinator.GetCoordinator(ctx)
	if err != nil {
		return nil, err
	}
	if coordAddr == "" {
		return nil, ErrNoRemoteCoordinator
	}
	conn, err := grpc.NewClient(coordAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return conn, nil
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
	newReferenceRelation := &rrelation.ReferenceRelation{
		TableName:     clauseNode.RelationName,
		SchemaVersion: 1,
		ShardIds:      shardIds,
	}
	masterCoordinatorConn, err := getMasterCoordinatorConn(ctx, localMngr)
	if err != nil {
		if errors.Is(err, ErrNoRemoteCoordinator) {
			return localMngr.CreateReferenceRelation(ctx, newReferenceRelation, nil)
		} else {
			return fmt.Errorf("can't get master coordinator: %s", err.Error())
		}
	}
	defer func() {
		if err := masterCoordinatorConn.Close(); err != nil {
			spqrlog.Zero.Debug().Err(err).Msg("failed to close connection")
		}
	}()

	masterCoordinator := coord.NewAdapter(masterCoordinatorConn)
	err = masterCoordinator.CreateReferenceRelation(ctx, newReferenceRelation, nil)
	if err != nil {
		return err
	}
	return nil
}

func innerAlterDistributionAttach(ctx context.Context, mngr meta.EntityMgr,
	distributionId string, rels []*distributions.DistributedRelation) error {
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
	err = mngr.AlterDistributionAttach(ctx, distributionId, rels)
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
	masterCoordinatorConn, err := getMasterCoordinatorConn(ctx, localMngr)
	if err != nil {
		if errors.Is(err, ErrNoRemoteCoordinator) {
			return innerAlterDistributionAttach(ctx, localMngr, distributionId, attachRelation)
		} else {
			return fmt.Errorf("can't get master coordinator: %s", err.Error())
		}
	}
	defer func() {
		if err := masterCoordinatorConn.Close(); err != nil {
			spqrlog.Zero.Debug().Err(err).Msg("failed to close connection")
		}
	}()
	return innerAlterDistributionAttach(ctx, coord.NewAdapter(masterCoordinatorConn),
		distributionId, attachRelation)
}
