package distributions

import (
	"context"

	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/rfqn"
)

type DistributionMgr interface {
	ListDistributions(ctx context.Context) ([]*Distribution, error)
	CreateDistribution(ctx context.Context, ds *Distribution) ([]qdb.QdbStatement, error)
	DropDistribution(ctx context.Context, id string) error
	GetDistribution(ctx context.Context, id string) (*Distribution, error)

	GetRelationDistribution(ctx context.Context, relation_name *rfqn.RelationFQN) (*Distribution, error)

	AlterDistributionAttach(ctx context.Context, id string, rels []*DistributedRelation) error
	AlterDistributionDetach(ctx context.Context, id string, relName *rfqn.RelationFQN) error
	AlterDistributedRelation(ctx context.Context, id string, rel *DistributedRelation) error
	AlterDistributedRelationSchema(ctx context.Context, id string, relName string, schemaName string) error
	AlterDistributedRelationDistributionKey(ctx context.Context, id string, relName string, distributionKey []DistributionKeyEntry) error

	CreateUniqueIndex(ctx context.Context, dsID string, idx *UniqueIndex) error
	DropUniqueIndex(ctx context.Context, idxID string) error
	ListUniqueIndexes(ctx context.Context) (map[string]*UniqueIndex, error)
	ListDistributionIndexes(ctx context.Context, dsID string) (map[string]*UniqueIndex, error)
	ListRelationIndexes(ctx context.Context, relName *rfqn.RelationFQN) (map[string]*UniqueIndex, error)
}
