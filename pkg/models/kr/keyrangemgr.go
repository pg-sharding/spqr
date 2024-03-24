package kr

import "context"

type SplitKeyRange struct {
	Bound     KeyRangeBound
	SourceID  string
	Krid      string
	SplitLeft bool
}

type MoveKeyRange struct {
	ShardId string
	Krid    string
}

type UniteKeyRange struct {
	BaseKeyRangeId      string
	AppendageKeyRangeId string
}

type KeyRangeMgr interface {
	ListKeyRanges(ctx context.Context, distribution string) ([]*KeyRange, error)
	ListAllKeyRanges(ctx context.Context) ([]*KeyRange, error)
	AddKeyRange(ctx context.Context, kr *KeyRange) error
	LockKeyRange(ctx context.Context, krid string) (*KeyRange, error)
	UnlockKeyRange(ctx context.Context, krid string) error
	Split(ctx context.Context, split *SplitKeyRange) error
	Unite(ctx context.Context, unite *UniteKeyRange) error
	Move(ctx context.Context, move *MoveKeyRange) error
	DropKeyRange(ctx context.Context, krid string) error
	DropKeyRangeAll(ctx context.Context) error
}
