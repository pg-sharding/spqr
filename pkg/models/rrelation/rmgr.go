package rrelation

import (
	"context"
)

type ReferenceRelationMgr interface {
	ListReferenceRelations(ctx context.Context) ([]*ReferenceRelation, error)
	CreateReferenceRelation(ctx context.Context, id string, e []*AutoIncrementEntry) error
	DropReferenceRelation(ctx context.Context, id string) error
}
