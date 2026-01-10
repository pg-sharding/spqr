package meta_test

import (
	"context"
	"testing"

	"github.com/pg-sharding/spqr/pkg/coord"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/stretchr/testify/assert"
)

var boolTrue bool = true
var boolFalse bool = false

var kr1 = &kr.KeyRange{
	ID:           "kr1",
	ShardID:      "sh1",
	Distribution: "ds1",
	LowerBound:   []any{int64(0)},
	ColumnTypes:  []string{qdb.ColumnTypeInteger},
}

var kr1_double = &kr.KeyRange{
	ID:           "kr1DOUBLE",
	ShardID:      "sh1",
	Distribution: "ds1",
	LowerBound:   []any{int64(0)},
	ColumnTypes:  []string{qdb.ColumnTypeInteger},
}

var kr2 = &kr.KeyRange{
	ID:           "kr2",
	ShardID:      "sh1",
	Distribution: "ds1",
	LowerBound:   []any{int64(10)},
	ColumnTypes:  []string{qdb.ColumnTypeInteger},
}
var kr2_sh2 = &kr.KeyRange{
	ID:           "kr2",
	ShardID:      "sh2",
	Distribution: "ds1",
	LowerBound:   []any{int64(10)},
	ColumnTypes:  []string{qdb.ColumnTypeInteger},
}

var kr1_locked = &kr.KeyRange{
	ID:           "kr1",
	ShardID:      "sh1",
	Distribution: "ds1",
	LowerBound:   []any{int64(0)},
	ColumnTypes:  []string{qdb.ColumnTypeInteger},
	IsLocked:     &boolTrue,
}

var kr1_not_locked = &kr.KeyRange{
	ID:           "kr1",
	ShardID:      "sh1",
	Distribution: "ds1",
	LowerBound:   []any{int64(0)},
	ColumnTypes:  []string{qdb.ColumnTypeInteger},
	IsLocked:     &boolFalse,
}

func prepareDbTestValidate(ctx context.Context) (*qdb.MemQDB, error) {
	memqdb, err := qdb.RestoreQDB(MemQDBPath)
	if err != nil {
		return nil, err
	}
	var chunk []qdb.QdbStatement
	if chunk, err = memqdb.CreateDistribution(ctx, qdb.NewDistribution("ds1", []string{qdb.ColumnTypeInteger})); err != nil {
		return nil, err
	}
	if err = memqdb.ExecNoTransaction(ctx, chunk); err != nil {
		return nil, err
	}
	if err = memqdb.AddShard(ctx, mockShard1); err != nil {
		return nil, err
	}
	if err = memqdb.AddShard(ctx, mockShard2); err != nil {
		return nil, err
	}
	return memqdb, nil
}

func TestValidateKeyRangeForCreate_happyPath(t *testing.T) {
	is := assert.New(t)
	ctx := context.TODO()
	memqdb, err := prepareDbTestValidate(ctx)
	assert.NoError(t, err)
	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil)

	assert.NoError(t, meta.ValidateKeyRangeForCreate(ctx, mngr, kr2))
	statements, err := mngr.CreateKeyRange(ctx, kr2)
	is.NoError(err)
	err = mngr.ExecNoTran(ctx, statements)
	is.NoError(err)
	is.NoError(meta.ValidateKeyRangeForCreate(ctx, mngr, kr1))
}
func TestValidateKeyRangeForCreate_intersectWithExistsSameShard(t *testing.T) {
	is := assert.New(t)
	ctx := context.TODO()
	memqdb, err := prepareDbTestValidate(ctx)
	assert.NoError(t, err)
	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil)

	is.NoError(meta.ValidateKeyRangeForCreate(ctx, mngr, kr1))
	statements, err := mngr.CreateKeyRange(ctx, kr1)
	is.NoError(err)
	err = mngr.ExecNoTran(ctx, statements)
	is.NoError(err)
	is.NoError(meta.ValidateKeyRangeForCreate(ctx, mngr, kr2))
}
func TestValidateKeyRangeForCreate_intersectWithExistsAnotherShard(t *testing.T) {
	is := assert.New(t)
	ctx := context.TODO()
	memqdb, err := prepareDbTestValidate(ctx)
	assert.NoError(t, err)
	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil)

	is.NoError(meta.ValidateKeyRangeForCreate(ctx, mngr, kr1))
	statements, err := mngr.CreateKeyRange(ctx, kr1)
	is.NoError(err)
	err = mngr.ExecNoTran(ctx, statements)
	is.NoError(err)
	is.Error(meta.ValidateKeyRangeForCreate(ctx, mngr, kr2_sh2),
		"key range kr2 intersects with key range kr1 in QDB")
}

func TestValidateKeyRangeForCreate_equalBound(t *testing.T) {
	is := assert.New(t)
	ctx := context.TODO()
	memqdb, err := prepareDbTestValidate(ctx)
	assert.NoError(t, err)
	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil)

	is.NoError(meta.ValidateKeyRangeForCreate(ctx, mngr, kr1))
	statements, err := mngr.CreateKeyRange(ctx, kr1)
	is.NoError(err)
	err = mngr.ExecNoTran(ctx, statements)
	is.NoError(err)
	is.Error(meta.ValidateKeyRangeForCreate(ctx, mngr, kr1_double),
		"key range kr1DOUBLE equals key range kr1 in QDB")
}

func TestValidateKeyRangeForModify_happyPath(t *testing.T) {
	is := assert.New(t)
	ctx := context.TODO()
	memqdb, err := prepareDbTestValidate(ctx)
	assert.NoError(t, err)
	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil)

	is.NoError(meta.ValidateKeyRangeForCreate(ctx, mngr, kr2))
	statements, err := mngr.CreateKeyRange(ctx, kr2)
	is.NoError(err)
	err = mngr.ExecNoTran(ctx, statements)
	is.NoError(err)
	is.NoError(meta.ValidateKeyRangeForCreate(ctx, mngr, kr1_locked))
}

func TestValidateKeyRangeForModify_lock_fail(t *testing.T) {
	is := assert.New(t)
	ctx := context.TODO()
	memqdb, err := prepareDbTestValidate(ctx)
	assert.NoError(t, err)
	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil)

	is.NoError(meta.ValidateKeyRangeForCreate(ctx, mngr, kr2))
	statements, err := mngr.CreateKeyRange(ctx, kr2)
	is.NoError(err)
	err = mngr.ExecNoTran(ctx, statements)
	is.NoError(err)
	is.NoError(meta.ValidateKeyRangeForCreate(ctx, mngr, kr1))
	statements, err = mngr.CreateKeyRange(ctx, kr1)
	is.NoError(err)
	err = mngr.ExecNoTran(ctx, statements)
	is.NoError(err)
	//lock unknown
	is.Error(meta.ValidateKeyRangeForModify(ctx, mngr, kr1))
	//not locked
	is.Error(meta.ValidateKeyRangeForModify(ctx, mngr, kr1_not_locked))
}

func TestValidateKeyRangeForModify_intersection(t *testing.T) {
	is := assert.New(t)
	ctx := context.TODO()
	memqdb, err := prepareDbTestValidate(ctx)
	assert.NoError(t, err)
	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil)

	is.NoError(meta.ValidateKeyRangeForCreate(ctx, mngr, kr2))
	statements, err := mngr.CreateKeyRange(ctx, kr2)
	is.NoError(err)
	err = mngr.ExecNoTran(ctx, statements)
	is.NoError(err)
	is.NoError(meta.ValidateKeyRangeForCreate(ctx, mngr, kr1))
	statements, err = mngr.CreateKeyRange(ctx, kr1)
	is.NoError(err)
	err = mngr.ExecNoTran(ctx, statements)
	is.NoError(err)

	is.Error(meta.ValidateKeyRangeForModify(ctx, mngr, kr1_double))
}
