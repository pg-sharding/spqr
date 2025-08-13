package ops_test

import (
	"context"
	"testing"

	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/qdb/ops"
	"github.com/stretchr/testify/assert"
)

const MemQDBPath = ""

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

func prepareDB(ctx context.Context) (*qdb.MemQDB, error) {
	memqdb, err := qdb.RestoreQDB(MemQDBPath)
	if err != nil {
		return nil, err
	}
	if err = memqdb.CreateDistribution(ctx, qdb.NewDistribution("ds1", nil)); err != nil {
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

func TestCreateKeyRangeWithChecks_happyPath(t *testing.T) {
	assert := assert.New(t)
	ctx := context.TODO()
	memqdb, err := prepareDB(ctx)
	assert.NoError(err)

	assert.NoError(ops.CreateKeyRangeWithChecks(ctx, memqdb, kr2))
	assert.NoError(ops.CreateKeyRangeWithChecks(ctx, memqdb, kr1))
}
func TestCreateKeyRangeWithChecks_intersectWithExistsSameShard(t *testing.T) {
	assert := assert.New(t)
	ctx := context.TODO()
	memqdb, err := prepareDB(ctx)
	assert.NoError(err)

	assert.NoError(ops.CreateKeyRangeWithChecks(ctx, memqdb, kr1))
	assert.NoError(ops.CreateKeyRangeWithChecks(ctx, memqdb, kr2))
}
func TestCreateKeyRangeWithChecks_intersectWithExistsAnotherShard(t *testing.T) {
	assert := assert.New(t)
	ctx := context.TODO()
	memqdb, err := prepareDB(ctx)
	assert.NoError(err)

	assert.NoError(ops.CreateKeyRangeWithChecks(ctx, memqdb, kr1))
	assert.Error(ops.CreateKeyRangeWithChecks(ctx, memqdb, kr2_sh2),
		"key range kr2 intersects with key range kr1 in QDB")
}

func TestCreateKeyRangeWithChecks_equalBound(t *testing.T) {
	assert := assert.New(t)
	ctx := context.TODO()
	memqdb, err := prepareDB(ctx)
	assert.NoError(err)

	assert.NoError(ops.CreateKeyRangeWithChecks(ctx, memqdb, kr1))
	assert.Error(ops.CreateKeyRangeWithChecks(ctx, memqdb, kr1_double),
		"key range kr1DOUBLE equals key range kr1 in QDB")
}
