package rrelation

import (
	"context"
)

type ReferenceRelationMgr interface {
	ListReferenceRelations(ctx context.Context) ([]*ReferenceRelation, error)
	CreateReferenceRelation(ctx context.Context, r *ReferenceRelation, e []*AutoIncrementEntry) error
	DropReferenceRelation(ctx context.Context, id string) error
}
