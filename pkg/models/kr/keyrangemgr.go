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

type MergeKeyRange struct {
	KeyRangeIDLeft  string
	KeyRangeIDRight string
}

type KeyRangeMgr interface {
	ListKeyRanges(ctx context.Context) ([]*KeyRange, error)
	AddKeyRange(ctx context.Context, kr *KeyRange) error
	LockKeyRange(ctx context.Context, krid string) (*KeyRange, error)
	UnlockKeyRange(ctx context.Context, krid string) error
	SplitKeyRange(ctx context.Context, split *SplitKeyRange) error
	MergeKeyRanges(ctx context.Context, merge *MergeKeyRange) error
	Move(ctx context.Context, move *MoveKeyRange) error
	DropKeyRange(ctx context.Context, krid string) error
	DropAllKeyRanges(ctx context.Context) ([]*KeyRange, error)
}
