package kr

import (
	"context"
	"github.com/pg-sharding/spqr/pkg/models/tasks"
)

type SplitKeyRange struct {
	Bound     [][]byte // KeyRangeBound raw
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

type RedistributeKeyLimit interface{}

type RedistributeAllKeys struct {
	RedistributeKeyLimit
}

type RedistributeKeyAmount struct {
	RedistributeKeyLimit

	Amount int64
}

type BatchMoveKeyRange struct {
	KrId      string
	ShardId   string
	Limit     RedistributeKeyLimit
	BatchSize int
	DestKrId  string

	Type tasks.SplitType
}

type RedistributeKeyRange struct {
	KrId      string
	ShardId   string
	BatchSize int
}

type KeyRangeMgr interface {
	GetKeyRange(ctx context.Context, krId string) (*KeyRange, error)
	ListKeyRanges(ctx context.Context, distribution string) ([]*KeyRange, error)
	ListAllKeyRanges(ctx context.Context) ([]*KeyRange, error)
	CreateKeyRange(ctx context.Context, kr *KeyRange) error
	LockKeyRange(ctx context.Context, krid string) (*KeyRange, error)
	UnlockKeyRange(ctx context.Context, krid string) error
	Split(ctx context.Context, split *SplitKeyRange) error
	Unite(ctx context.Context, unite *UniteKeyRange) error
	Move(ctx context.Context, move *MoveKeyRange) error
	DropKeyRange(ctx context.Context, krid string) error
	DropKeyRangeAll(ctx context.Context) error
	BatchMoveKeyRange(ctx context.Context, req *BatchMoveKeyRange) error
	RedistributeKeyRange(ctx context.Context, req *RedistributeKeyRange) error
	RenameKeyRange(ctx context.Context, krId, krIdNew string) error
}
