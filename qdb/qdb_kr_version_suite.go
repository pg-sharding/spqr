package qdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func RunTestKeyRangeChangeVersion(t *testing.T, qdb XQDB) {
	assert := assert.New(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	statements, err := qdb.CreateDistribution(ctx, NewDistribution("ds1", []string{"integer"}))
	assert.NoError(err)
	assert.NoError(qdb.ExecNoTransaction(ctx, statements))
	statements, err = qdb.CreateDistribution(ctx, NewDistribution("ds2", []string{"integer"}))
	assert.NoError(err)
	assert.NoError(qdb.ExecNoTransaction(ctx, statements))

	err = qdb.AddShard(ctx, NewShard("sh1", []string{"localhost:6432"}, nil))
	assert.NoError(err)

	testkr := &KeyRange{
		KeyRangeID:     "testkr",
		DistributionId: "ds1",
		ShardID:        "sh1",
	}
	statements, err = qdb.CreateKeyRange(ctx, testkr)
	assert.NoError(err)
	err = qdb.ExecNoTransaction(ctx, statements)
	assert.NoError(err)

	testkr2 := &KeyRange{
		KeyRangeID:     "testkr2",
		DistributionId: "ds1",
		ShardID:        "sh1",
	}
	statements, err = qdb.CreateKeyRange(ctx, testkr2)
	assert.NoError(err)
	err = qdb.ExecNoTransaction(ctx, statements)
	assert.NoError(err)

	kr, err := qdb.GetKeyRange(ctx, testkr.KeyRangeID)
	assert.NoError(err)
	assert.Equal(1, kr.Version)

	// Update distribution ID
	kr.DistributionId = "ds2"
	statements, err = qdb.UpdateKeyRange(ctx, kr)
	assert.NoError(err)
	err = qdb.ExecNoTransaction(ctx, statements)
	assert.NoError(err)

	kr, err = qdb.GetKeyRange(ctx, kr.KeyRangeID)
	assert.NoError(err)
	assert.Equal(2, kr.Version)

	// Lock key range does not change version
	kr, err = qdb.LockKeyRange(ctx, kr.KeyRangeID)
	assert.NoError(err)

	kr, err = qdb.GetKeyRange(ctx, kr.KeyRangeID)
	assert.NoError(err)
	assert.Equal(2, kr.Version)

	// Check other key ranges were not updated
	kr, err = qdb.GetKeyRange(ctx, testkr2.KeyRangeID)
	assert.NoError(err)
	assert.Equal(1, kr.Version)
}
