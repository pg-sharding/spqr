package kr

import (
	"context"

	"github.com/pg-sharding/spqr/pkg/models/tasks"
	"github.com/pg-sharding/spqr/qdb"
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
	TaskGroupId string // if empty, will be a random uuid. Otherwise user-input
	KeyRangeId  string // KeyRangeId is the source key range id
	ShardId     string // ShardId is the destination shard id
	Limit       int64  /* Limit is kr.RedistributeKeyLimit value specifying the number of keys to transfer.
	Can be either negative, in which case the whole key range will be moved,
	or non-negative, where circa specified amount of keys will be moved. */
	BatchSize int    // BatchSize is the amount of keys to be transferred in every transaction.
	DestKrId  string /* DestKrId is the destination key range id.
	If the whole key range is being moved, it's still renamed. */

	Type tasks.SplitType // Type is the tasks.SplitType value specifying if leftmost or rightmost portion of the key range will be moved
}

type RedistributeKeyRange struct {
	TaskGroupId string // optional id.
	KrId        string // KrId is the source key range id
	ShardId     string // ShardId is the destination shard id
	BatchSize   int    // BatchSize is the amount of keys to be transferred in every transaction.
	Check       bool   // if Check is set, we perform a pre-run check for the ability to redistribute
	Apply       bool   // if Apply is not set, command will be a dry-run
	NoWait      bool   // do we wait for redistribute completion?
}

type KeyRangeMgr interface {
	GetKeyRange(ctx context.Context, krId string) (*KeyRange, error)
	ListKeyRanges(ctx context.Context, distribution string) ([]*KeyRange, error)
	ListAllKeyRanges(ctx context.Context) ([]*KeyRange, error)
	ListKeyRangeLocks(ctx context.Context) ([]string, error)
	CreateKeyRange(ctx context.Context, kr *KeyRange) ([]qdb.QdbStatement, error)
	LockKeyRange(ctx context.Context, krid string) (*KeyRange, error)
	UnlockKeyRange(ctx context.Context, krid string) error
	Split(ctx context.Context, split *SplitKeyRange) error
	Unite(ctx context.Context, unite *UniteKeyRange) error
	Move(ctx context.Context, move *MoveKeyRange) error
	DropKeyRange(ctx context.Context, krid string) error
	DropKeyRangeAll(ctx context.Context) error
	BatchMoveKeyRange(ctx context.Context, req *BatchMoveKeyRange, issuer *tasks.MoveTaskGroupIssuer) error
	RedistributeKeyRange(ctx context.Context, req *RedistributeKeyRange) error
	RenameKeyRange(ctx context.Context, krId, krIdNew string) error
}
