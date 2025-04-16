package sequences

import (
	"context"
)

type SequenceMgr interface {
	DropSequence(ctx context.Context, name string) error
}
