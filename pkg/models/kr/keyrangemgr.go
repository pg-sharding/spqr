package kr

import "context"

type SplitKeyRange struct {
	Bound    KeyRangeBound
	SourceID string
	Krid     string
}

type MoveKeyRange struct {
	ShardId string
	Krid    string
}

type UniteKeyRange struct {
	KeyRangeIDLeft  string
	KeyRangeIDRight string
}

type KeyRangeMgr interface {
	ListKeyRanges(ctx context.Context) ([]*KeyRange, error)

	AddKeyRange(ctx context.Context, kr *KeyRange) error

	LockKeyRange(ctx context.Context, krid string) (*KeyRange, error)
	Unlock(ctx context.Context, krid string) error

	Split(ctx context.Context, split *SplitKeyRange) error
	Unite(ctx context.Context, unite *UniteKeyRange) error
	Move(ctx context.Context, move *MoveKeyRange) error

	DropKeyRange(ctx context.Context, krid string) error
	DropKeyRangeAll(ctx context.Context) ([]*KeyRange, error)
}
