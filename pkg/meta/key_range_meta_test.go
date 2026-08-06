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
	IsLocked:     true,
}

var kr1NotLocked = &kr.KeyRange{
	ID:           "kr1",
	ShardID:      "sh1",
	Distribution: "ds1",
	LowerBound:   []any{int64(0)},
	ColumnTypes:  []string{qdb.ColumnTypeInteger},
	IsLocked:     false,
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

	var spErr *spqrerror.SpqrError
	if is.ErrorAs(err, &spErr) {
		is.Equal(spqrerror.SPQR_NO_DATASHARD, spErr.ErrorCode)
		is.Equal("Shard \"nonexistentshard\" not found.", spErr.Error())
		is.Equal("Run 'SHOW shards' to see all configured shards.", spErr.ErrHint)
	}
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

	var spErr *spqrerror.SpqrError
	if is.ErrorAs(err, &spErr) {
		is.Equal(spqrerror.SPQR_NO_DATASHARD, spErr.ErrorCode)
		is.Equal("Shard \"nonexistentshard\" not found.", spErr.Error())
		is.Equal("Run 'SHOW shards' to see all configured shards.", spErr.ErrHint)
	}
}

func TestSplitEqualFullKeyRange(t *testing.T) {
	tests := []struct {
		name     string
		colTypes []string
		shards   int
		rng      *kr.CustomDataTypeRange
		expected []kr.KeyRangeBound
	}{
		{
			"uinteger with 1 shard [0-100]",
			[]string{qdb.ColumnTypeUinteger},
			1,
			&kr.CustomDataTypeRange{
				LowerBound: kr.KeyRangeBound{uint64(0)},
				UpperBound: kr.KeyRangeBound{uint64(100)},
			},
			[]kr.KeyRangeBound{{uint64(0)}},
		},
		{
			"uinteger with 2 shards [0-100]",
			[]string{qdb.ColumnTypeUinteger},
			2,
			&kr.CustomDataTypeRange{
				LowerBound: kr.KeyRangeBound{uint64(0)},
				UpperBound: kr.KeyRangeBound{uint64(100)},
			},
			[]kr.KeyRangeBound{
				{uint64(50)},
				{uint64(0)},
			},
		},
		{
			"uinteger with 4 shards [0-100]",
			[]string{qdb.ColumnTypeUinteger},
			4,
			&kr.CustomDataTypeRange{
				LowerBound: kr.KeyRangeBound{uint64(0)},
				UpperBound: kr.KeyRangeBound{uint64(100)},
			},
			[]kr.KeyRangeBound{
				{uint64(75)},
				{uint64(50)},
				{uint64(25)},
				{uint64(0)},
			},
		},
		{
			"uinteger with 4 shards",
			[]string{qdb.ColumnTypeUinteger},
			4,
			nil,
			[]kr.KeyRangeBound{
				{uint64(13835058055282163709)},
				{uint64(9223372036854775806)},
				{uint64(4611686018427387903)},
				{uint64(0)},
			},
		},

		{
			"integer with 1 shard [0-100]",
			[]string{qdb.ColumnTypeInteger},
			1,
			&kr.CustomDataTypeRange{
				LowerBound: kr.KeyRangeBound{int64(0)},
				UpperBound: kr.KeyRangeBound{int64(100)},
			},
			[]kr.KeyRangeBound{
				{int64(0)},
			},
		},
		{
			"integer with 4 shards [-100 - 100]",
			[]string{qdb.ColumnTypeInteger},
			4,
			&kr.CustomDataTypeRange{
				LowerBound: kr.KeyRangeBound{int64(-100)},
				UpperBound: kr.KeyRangeBound{int64(100)},
			},
			[]kr.KeyRangeBound{
				{int64(50)},
				{int64(0)},
				{int64(-50)},
				{int64(-100)},
			},
		},
		{
			"integer with 4 shards",
			[]string{qdb.ColumnTypeInteger},
			4,
			nil,
			[]kr.KeyRangeBound{
				{int64(4611686018427387901)},
				{int64(-2)},
				{int64(-4611686018427387905)},
				{int64(-9223372036854775808)},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			krs, err := meta.SplitEqualFullKeyRange(tc.colTypes, tc.shards, tc.rng)
			assert.NoError(t, err)
			assert.EqualValues(t, tc.expected, krs)
		})
	}
}
