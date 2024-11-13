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

type BatchMoveKeyRange struct {
	KrId    string // KrId is the source key range id
	ShardId string // ShardId is the destination shard id
	Limit   int64  /* Limit is kr.RedistributeKeyLimit value specifying the number of keys to transfer.
	Can be either negative, in which case the whole key range will be moved,
	or non-negative, where circa specified amount of keys will be moved. */
	BatchSize int    // BatchSize is the amount of keys to be transferred in every transaction.
	DestKrId  string /* DestKrId is the destination key range id.
	If the whole key range is being moved, it's still renamed. */

	Type tasks.SplitType // Type is the tasks.SplitType value specifying if leftmost or rightmost portion of the key range will be moved
}

type RedistributeKeyRange struct {
	KrId      string // KrId is the source key range id
	ShardId   string // ShardId is the destination shard id
	BatchSize int    // BatchSize is the amount of keys to be transferred in every transaction.
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
