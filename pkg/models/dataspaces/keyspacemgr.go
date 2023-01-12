package dataspaces

import (
	"context"
)

type DataspaceMgr interface {
	ListDataspace(ctx context.Context) ([]*Dataspace, error)
	AddDataspace(ctx context.Context, ks *Dataspace) error
}
