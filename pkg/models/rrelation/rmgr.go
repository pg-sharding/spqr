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

	SyncReferenceRelations(ctx context.Context, ids []*rfqn.RelationFQN, destShard string) error
}
