package kr

import "context"

type SplitKeyRange struct {
	Bound    KeyRangeBound
	SourceID string
	Krid     string
}

type UniteKeyRange struct {
	KeyRangeIDLeft  string
	KeyRangeIDRight string
}

type KeyRangeMgr interface {
	ListKeyRanges(ctx context.Context) ([]*KeyRange, error)

	AddKeyRange(ctx context.Context, kr *KeyRange) error

	Lock(ctx context.Context, krid string) (*KeyRange, error)
	Unlock(ctx context.Context, krid string) error

	Split(ctx context.Context, req *SplitKeyRange) error
	Unite(ctx context.Context, req *UniteKeyRange) error
}
