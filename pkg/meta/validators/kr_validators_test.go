package meta_validators_test

import (
	"context"
	"testing"

	"github.com/pg-sharding/spqr/pkg/coord"
	validator "github.com/pg-sharding/spqr/pkg/meta/validators"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/stretchr/testify/assert"
)

const MemQDBPath = ""

var boolTrue bool = true
var boolFalse bool = false

var mockShard1 = &qdb.Shard{
	ID:       "sh1",
	RawHosts: []string{"host1", "host2"},
}
var mockShard2 = &qdb.Shard{
	ID:       "sh2",
	RawHosts: []string{"host3", "host4"},
}

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

func prepareDB(ctx context.Context) (*qdb.MemQDB, error) {
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
	ctx := context.TODO()
	memqdb, err := prepareDB(ctx)
	assert.NoError(err)
	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil)

	assert.NoError(validator.ValidateKeyRangeForCreate(ctx, mngr, kr2))
	assert.NoError(mngr.CreateKeyRange(ctx, kr2))
	assert.NoError(validator.ValidateKeyRangeForCreate(ctx, mngr, kr1))
}
func TestValidateKeyRangeForCreate_intersectWithExistsSameShard(t *testing.T) {
	ctx := context.TODO()
	memqdb, err := prepareDB(ctx)
	assert.NoError(err)
	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil)

	assert.NoError(validator.ValidateKeyRangeForCreate(ctx, mngr, kr1))
	assert.NoError(mngr.CreateKeyRange(ctx, kr1))
	assert.NoError(validator.ValidateKeyRangeForCreate(ctx, mngr, kr2))
}
func TestValidateKeyRangeForCreate_intersectWithExistsAnotherShard(t *testing.T) {
	ctx := context.TODO()
	memqdb, err := prepareDB(ctx)
	assert.NoError(err)
	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil)

	assert.NoError(validator.ValidateKeyRangeForCreate(ctx, mngr, kr1))
	assert.NoError(mngr.CreateKeyRange(ctx, kr1))
	assert.Error(validator.ValidateKeyRangeForCreate(ctx, mngr, kr2_sh2),
		"key range kr2 intersects with key range kr1 in QDB")
}

func TestValidateKeyRangeForCreate_equalBound(t *testing.T) {
	ctx := context.TODO()
	memqdb, err := prepareDB(ctx)
	assert.NoError(err)
	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil)

	assert.NoError(validator.ValidateKeyRangeForCreate(ctx, mngr, kr1))
	assert.NoError(mngr.CreateKeyRange(ctx, kr1))
	assert.Error(validator.ValidateKeyRangeForCreate(ctx, mngr, kr1_double),
		"key range kr1DOUBLE equals key range kr1 in QDB")
}

func TestValidateKeyRangeForModify_happyPath(t *testing.T) {
	ctx := context.TODO()
	memqdb, err := prepareDB(ctx)
	assert.NoError(err)
	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil)

	assert.NoError(validator.ValidateKeyRangeForCreate(ctx, mngr, kr2))
	assert.NoError(mngr.CreateKeyRange(ctx, kr2))
	assert.NoError(validator.ValidateKeyRangeForCreate(ctx, mngr, kr1_locked))
}

func TestValidateKeyRangeForModify_lock_fail(t *testing.T) {
	ctx := context.TODO()
	memqdb, err := prepareDB(ctx)
	assert.NoError(err)
	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil)

	assert.NoError(validator.ValidateKeyRangeForCreate(ctx, mngr, kr2))
	assert.NoError(mngr.CreateKeyRange(ctx, kr2))
	assert.NoError(validator.ValidateKeyRangeForCreate(ctx, mngr, kr1))
	assert.NoError(mngr.CreateKeyRange(ctx, kr1))
	//lock unknown
	assert.Error(validator.ValidateKeyRangeForModify(ctx, mngr, kr1))
	//not locked
	assert.Error(validator.ValidateKeyRangeForModify(ctx, mngr, kr1_not_locked))
}

func TestValidateKeyRangeForModify_intersection(t *testing.T) {
	ctx := context.TODO()
	memqdb, err := prepareDB(ctx)
	assert.NoError(err)
	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil)

	assert.NoError(validator.ValidateKeyRangeForCreate(ctx, mngr, kr2))
	assert.NoError(mngr.CreateKeyRange(ctx, kr2))
	assert.NoError(validator.ValidateKeyRangeForCreate(ctx, mngr, kr1))
	assert.NoError(mngr.CreateKeyRange(ctx, kr1))

	assert.Error(validator.ValidateKeyRangeForModify(ctx, mngr, kr1_double))
}
