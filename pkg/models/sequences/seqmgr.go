package sequences

import (
	"context"

	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/rfqn"
)

type SequenceMgr interface {
	ListSequences(ctx context.Context) ([]string, error)
	ListRelationSequences(ctx context.Context, rel *rfqn.RelationFQN) (map[string]string, error)
	GetSequenceColumns(ctx context.Context, seqName string) ([]string, error)
	NextRange(ctx context.Context, seqName string, rangeSize uint64) (*qdb.SequenceIdRange, error)
	CurrVal(ctx context.Context, seqName string) (int64, error)

	DropSequence(ctx context.Context, name string, force bool) error
}
