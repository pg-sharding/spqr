package meta_test

import (
	"context"
	"testing"

	"github.com/pg-sharding/spqr/pkg/coord"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/stretchr/testify/assert"
)

var boolTrue = true
var boolFalse = false

var ds1ColTypes = []string{qdb.ColumnTypeInteger}

var kr1 = &kr.KeyRange{
	ID:           "kr1",
	ShardID:      "sh1",
	Distribution: "ds1",
	LowerBound:   []any{int64(0)},
	ColumnTypes:  []string{qdb.ColumnTypeInteger},
}

var kr1Double = &kr.KeyRange{
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
var kr2Sh2 = &kr.KeyRange{
	ID:           "kr2",
	ShardID:      "sh2",
	Distribution: "ds1",
	LowerBound:   []any{int64(10)},
	ColumnTypes:  []string{qdb.ColumnTypeInteger},
}

var kr1Locked = &kr.KeyRange{
	ID:           "kr1",
	ShardID:      "sh1",
	Distribution: "ds1",
	LowerBound:   []any{int64(0)},
	ColumnTypes:  []string{qdb.ColumnTypeInteger},
	IsLocked:     &boolTrue,
}

var kr1NotLocked = &kr.KeyRange{
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
	if chunk, err = memqdb.CreateDistribution(ctx, qdb.NewDistribution("ds2", []string{qdb.ColumnTypeInteger})); err != nil {
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
	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil, topology.TopMgrFromMap(map[string]*topology.DataShard{}), false, nil, qdb.DefaultMaxTxnSize)

	assert.NoError(t, meta.ValidateKeyRangeForCreate(ctx, mngr, kr2))
	tranMngr := meta.NewTranEntityManager(mngr)
	err = tranMngr.CreateKeyRange(ctx, kr2, ds1ColTypes)
	is.NoError(err)
	err = tranMngr.ExecNoTran(ctx)
	is.NoError(err)
	is.NoError(meta.ValidateKeyRangeForCreate(ctx, mngr, kr1))
}
func TestValidateKeyRangeForCreate_intersectWithExistsSameShard(t *testing.T) {
	is := assert.New(t)
	ctx := context.TODO()
	memqdb, err := prepareDbTestValidate(ctx)
	assert.NoError(t, err)
	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil, topology.TopMgrFromMap(map[string]*topology.DataShard{}), false, nil, qdb.DefaultMaxTxnSize)

	is.NoError(meta.ValidateKeyRangeForCreate(ctx, mngr, kr1))
	tranMngr := meta.NewTranEntityManager(mngr)
	err = tranMngr.CreateKeyRange(ctx, kr1, ds1ColTypes)
	is.NoError(err)
	err = tranMngr.ExecNoTran(ctx)
	is.NoError(err)
	is.NoError(meta.ValidateKeyRangeForCreate(ctx, mngr, kr2))
}
func TestValidateKeyRangeForCreate_intersectWithExistsAnotherShard(t *testing.T) {
	is := assert.New(t)
	ctx := context.TODO()
	memqdb, err := prepareDbTestValidate(ctx)
	assert.NoError(t, err)
	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil, topology.TopMgrFromMap(map[string]*topology.DataShard{}), false, nil, qdb.DefaultMaxTxnSize)

	is.NoError(meta.ValidateKeyRangeForCreate(ctx, mngr, kr1))
	tranMngr := meta.NewTranEntityManager(mngr)
	err = tranMngr.CreateKeyRange(ctx, kr1, ds1ColTypes)
	is.NoError(err)
	err = tranMngr.ExecNoTran(ctx)
	is.NoError(err)
	is.Error(meta.ValidateKeyRangeForCreate(ctx, mngr, kr2Sh2),
		"key range kr2 intersects with key range kr1 in QDB")
}

func TestValidateKeyRangeForCreate_equalBound(t *testing.T) {
	is := assert.New(t)
	ctx := context.TODO()
	memqdb, err := prepareDbTestValidate(ctx)
	assert.NoError(t, err)
	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil, topology.TopMgrFromMap(map[string]*topology.DataShard{}), false, nil, qdb.DefaultMaxTxnSize)

	is.NoError(meta.ValidateKeyRangeForCreate(ctx, mngr, kr1))
	tranMngr := meta.NewTranEntityManager(mngr)
	err = tranMngr.CreateKeyRange(ctx, kr1, ds1ColTypes)
	is.NoError(err)
	err = tranMngr.ExecNoTran(ctx)
	is.NoError(err)
	is.Error(meta.ValidateKeyRangeForCreate(ctx, mngr, kr1Double),
		"key range kr1DOUBLE equals key range kr1 in QDB")
}

func TestValidateKeyRangeForModify_happyPath(t *testing.T) {
	is := assert.New(t)
	ctx := context.TODO()
	memqdb, err := prepareDbTestValidate(ctx)
	assert.NoError(t, err)
	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil, topology.TopMgrFromMap(map[string]*topology.DataShard{}), false, nil, qdb.DefaultMaxTxnSize)

	is.NoError(meta.ValidateKeyRangeForCreate(ctx, mngr, kr2))
	tranMngr := meta.NewTranEntityManager(mngr)
	err = tranMngr.CreateKeyRange(ctx, kr2, ds1ColTypes)
	is.NoError(err)
	err = tranMngr.ExecNoTran(ctx)
	is.NoError(err)
	is.NoError(meta.ValidateKeyRangeForCreate(ctx, mngr, kr1Locked))
}

func TestValidateKeyRangeForModify_lock_fail(t *testing.T) {
	is := assert.New(t)
	ctx := context.TODO()
	memqdb, err := prepareDbTestValidate(ctx)
	assert.NoError(t, err)
	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil, topology.TopMgrFromMap(map[string]*topology.DataShard{}), false, nil, qdb.DefaultMaxTxnSize)

	is.NoError(meta.ValidateKeyRangeForCreate(ctx, mngr, kr2))
	tranMngr2 := meta.NewTranEntityManager(mngr)
	err = tranMngr2.CreateKeyRange(ctx, kr2, ds1ColTypes)
	is.NoError(err)
	err = tranMngr2.ExecNoTran(ctx)
	is.NoError(err)
	is.NoError(meta.ValidateKeyRangeForCreate(ctx, mngr, kr1))
	tranMngr1 := meta.NewTranEntityManager(mngr)
	err = tranMngr1.CreateKeyRange(ctx, kr1, ds1ColTypes)
	is.NoError(err)
	err = tranMngr1.ExecNoTran(ctx)
	is.NoError(err)
	//lock unknown
	is.Error(meta.ValidateKeyRangeForModify(ctx, mngr, kr1))
	//not locked
	is.Error(meta.ValidateKeyRangeForModify(ctx, mngr, kr1NotLocked))
}

func TestValidateKeyRangeForModify_intersection(t *testing.T) {
	is := assert.New(t)
	ctx := context.TODO()
	memqdb, err := prepareDbTestValidate(ctx)
	assert.NoError(t, err)
	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil, topology.TopMgrFromMap(map[string]*topology.DataShard{}), false, nil, qdb.DefaultMaxTxnSize)

	is.NoError(meta.ValidateKeyRangeForCreate(ctx, mngr, kr2))
	tranMngr2 := meta.NewTranEntityManager(mngr)
	err = tranMngr2.CreateKeyRange(ctx, kr2, ds1ColTypes)
	is.NoError(err)
	err = tranMngr2.ExecNoTran(ctx)
	is.NoError(err)
	is.NoError(meta.ValidateKeyRangeForCreate(ctx, mngr, kr1))
	tranMngr1 := meta.NewTranEntityManager(mngr)
	err = tranMngr1.CreateKeyRange(ctx, kr1, ds1ColTypes)
	is.NoError(err)
	err = tranMngr1.ExecNoTran(ctx)
	is.NoError(err)

	is.Error(meta.ValidateKeyRangeForModify(ctx, mngr, kr1Double))
}

func TestValidateKeyRangeForCreate_unknownShardReturnsHint(t *testing.T) {
	is := assert.New(t)
	ctx := context.TODO()

	memqdb, err := prepareDbTestValidate(ctx)
	is.NoError(err)
	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil, topology.TopMgrFromMap(map[string]*topology.DataShard{}), false, nil, qdb.DefaultMaxTxnSize)

	reqKr := &kr.KeyRange{
		ID:           "kr_missing",
		ShardID:      "nonexistentshard",
		Distribution: "ds1",
		LowerBound:   []any{int64(100)},
		ColumnTypes:  []string{qdb.ColumnTypeInteger},
	}

	err = meta.ValidateKeyRangeForCreate(ctx, mngr, reqKr)
	is.Error(err)

	spErr, ok := err.(*spqrerror.SpqrError)
	is.True(ok)
	is.Equal(spqrerror.SPQR_NO_DATASHARD, spErr.ErrorCode)
	is.Equal("Shard \"nonexistentshard\" not found.", spErr.Error())
	is.Equal("Run 'SHOW shards' to see all configured shards.", spErr.ErrHint)
}

func TestValidateKeyRangeForModify_unknownShardReturnsHint(t *testing.T) {
	is := assert.New(t)
	ctx := context.TODO()

	memqdb, err := prepareDbTestValidate(ctx)
	is.NoError(err)

	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil, topology.TopMgrFromMap(map[string]*topology.DataShard{}), false, nil, qdb.DefaultMaxTxnSize)
	tranMngr := meta.NewTranEntityManager(mngr)

	err = tranMngr.CreateKeyRange(ctx, kr1, ds1ColTypes)
	is.NoError(err)
	err = tranMngr.ExecNoTran(ctx)
	is.NoError(err)

	_, err = mngr.LockKeyRange(ctx, kr1.ID)
	is.NoError(err)

	reqKr := &kr.KeyRange{
		ID:           kr1.ID,
		ShardID:      "nonexistentshard",
		Distribution: kr1.Distribution,
		LowerBound:   kr1.LowerBound,
		ColumnTypes:  kr1.ColumnTypes,
	}

	err = meta.ValidateKeyRangeForModify(ctx, mngr, reqKr)
	is.Error(err)

	spErr, ok := err.(*spqrerror.SpqrError)
	is.True(ok)
	is.Equal(spqrerror.SPQR_NO_DATASHARD, spErr.ErrorCode)
	is.Equal("Shard \"nonexistentshard\" not found.", spErr.Error())
	is.Equal("Run 'SHOW shards' to see all configured shards.", spErr.ErrHint)
}
