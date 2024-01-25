package distributions

import (
	"context"
)

type DistributionMgr interface {
	ListDistribution(ctx context.Context) ([]*Distribution, error)
	CreateDistribution(ctx context.Context, ds *Distribution) error
	DropDistribution(ctx context.Context, id string) error
	GetDistribution(ctx context.Context, id string) (*Distribution, error)

	GetRelationDistribution(ctx context.Context, relation_name string) (*Distribution, error)

	AlterDistributionAttach(ctx context.Context, id string, rels []*DistributedRelatiton) error
}
