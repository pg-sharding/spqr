package sequences

import (
	"context"

	"github.com/pg-sharding/spqr/router/rfqn"
)

type SequenceMgr interface {
	ListSequences(ctx context.Context) ([]string, error)
	ListRelationSequences(ctx context.Context, rel *rfqn.RelationFQN) (map[string]string, error)
	NextVal(ctx context.Context, seqName string) (int64, error)
	CurrVal(ctx context.Context, seqName string) (int64, error)

	DropSequence(ctx context.Context, name string) error
}
