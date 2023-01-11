package keyspaces

import (
	"context"
)

type KeyspaceMgr interface {
	ListKeyspace(ctx context.Context) ([]*Keyspace, error)
	AddKeyspace(ctx context.Context, ks *Keyspace) error
}
