package rrelation

import "context"

type ReferenceRelationMgr interface {
	ListReferenceRelations(ctx context.Context) ([]*ReferenceRelation, error)
	CreateReferenceRelation(ctx context.Context, ds *ReferenceRelation) error
	DropReferenceRelation(ctx context.Context, id string) error
}
