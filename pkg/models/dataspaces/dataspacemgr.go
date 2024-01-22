package dataspaces

import (
	"context"
)

type DataspaceMgr interface {
	ListDataspace(ctx context.Context) ([]*Keyspace, error)
	AddDataspace(ctx context.Context, ds *Keyspace) error
	DropDataspace(ctx context.Context, ds *Keyspace) error

	AlterDataspaceAttachRelation(ctx context.Context, dsid string, rels map[string]ShardedRelation) error

	GetDataspace(ctx context.Context, id string) (*Keyspace, error)
	GetDataspaceForRelation(ctx context.Context, relation string) (*Keyspace, error)
}
