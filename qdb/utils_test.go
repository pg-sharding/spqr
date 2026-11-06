package qdb_test

import (
	"context"
	"testing"

	"github.com/pg-sharding/spqr/qdb"
	"github.com/stretchr/testify/assert"
)

func TestGetQDBStateHash(t *testing.T) {
	assert := assert.New(t)

	ctx := context.Background()
	memqdb, err := qdb.RestoreQDB("")
	assert.NoError(err)

	hash, err := qdb.GetQDBStateHash(ctx, memqdb)
	assert.NoError(err)
	assert.Equal(uint64(0), hash)

	chunk, err := memqdb.CreateDistribution(ctx, qdb.NewDistribution("ds1", nil))
	assert.NoError(err)
	err = memqdb.ExecNoTransaction(ctx, chunk)
	assert.NoError(err)

	relation := &qdb.DistributedRelation{
		Name: "r1",
		DistributionKey: []qdb.DistributionKeyEntry{
			{
				Column:       "c1",
				HashFunction: "",
			},
		},
	}
	assert.NoError(memqdb.AlterDistributionAttach(ctx, "ds1", []*qdb.DistributedRelation{
		relation,
	}))

	statements, err := memqdb.CreateKeyRange(ctx, &qdb.KeyRange{
		LowerBound:     [][]byte{[]byte("1111")},
		ShardID:        "sh1",
		KeyRangeID:     "krid1",
		DistributionId: "ds1",
	})
	assert.NoError(err)
	assert.NoError(memqdb.ExecNoTransaction(ctx, statements))

	hash, err = qdb.GetQDBStateHash(ctx, memqdb)
	assert.NoError(err)
	// The state now carries created_at/updated_at wall-clock stamps, so the
	// hash is no longer a fixed constant; pin the properties that matter: a
	// second computation over unchanged state is identical, and the hash moved
	// away from the empty-state value.
	again, err := qdb.GetQDBStateHash(ctx, memqdb)
	assert.NoError(err)
	assert.Equal(hash, again)
	assert.NotEqual(uint64(0), hash)
}
