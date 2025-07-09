package rrelation

import (
	"context"

	"github.com/pg-sharding/spqr/router/rfqn"
)

type ReferenceRelationMgr interface {
	ListReferenceRelations(ctx context.Context) ([]*ReferenceRelation, error)
	CreateReferenceRelation(ctx context.Context, r *ReferenceRelation, e []*AutoIncrementEntry) error
	GetReferenceRelation(ctx context.Context, relName *rfqn.RelationFQN) (*ReferenceRelation, error)
	DropReferenceRelation(ctx context.Context, relName *rfqn.RelationFQN) error

	/* Method for managing routers metadata */
	AlterReferenceRelationStorage(ctx context.Context, relName *rfqn.RelationFQN, shs []string) error

	/* Method for cluster-wide reference relation data replication */
	SyncReferenceRelations(ctx context.Context, ids []*rfqn.RelationFQN, destShard string) error
}
