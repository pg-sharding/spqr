package rrelation

import (
	"context"
)

type ReferenceRelationMgr interface {
	ListReferenceRelations(ctx context.Context) ([]*ReferenceRelation, error)
	CreateReferenceRelation(ctx context.Context, r *ReferenceRelation, e []*AutoIncrementEntry) error
	GetReferenceRelation(ctx context.Context, tableName string) (*ReferenceRelation, error)
	DropReferenceRelation(ctx context.Context, id string) error

	SyncReferenceRelations(ctx context.Context, ids []string, destShard string) error
}
