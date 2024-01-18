package dataspaces

import (
	"context"
)

type DataspaceMgr interface {
	ListDataspace(ctx context.Context) ([]*Dataspace, error)
	AddDataspace(ctx context.Context, ds *Dataspace) error
	DropDataspace(ctx context.Context, ds *Dataspace) error
	AttachToDataspace(ctx context.Context, table string, ds *Dataspace) error
	GetDataspace(ctx context.Context, table string) (*Dataspace, error)
}
