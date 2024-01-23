package dataspaces

import (
	"context"
)

type KeyspaceMgr interface {
	ListKeyspace(ctx context.Context) ([]*Keyspace, error)
	AddKeyspace(ctx context.Context, ds *Keyspace) error
	DropKeyspace(ctx context.Context, ds *Keyspace) error

	AlterKeyspaceAttachRelation(ctx context.Context, dsid string, rels map[string]ShardedRelation) error

	GetKeyspace(ctx context.Context, id string) (*Keyspace, error)
	GetKeyspaceForRelation(ctx context.Context, relation string) (*Keyspace, error)
}
