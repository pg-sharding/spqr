package sequences

import (
	"context"
)

type SequenceMgr interface {
	ListSequences(ctx context.Context) ([]string, error)
	NextVal(ctx context.Context, seqName string) (int64, error)
	CurrVal(ctx context.Context, seqName string) (int64, error)

	DropSequence(ctx context.Context, name string) error
}
