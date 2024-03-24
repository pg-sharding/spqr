package qdb_test

import (
	"context"
	"sync"
	"testing"

	"github.com/pg-sharding/spqr/qdb"
	"github.com/stretchr/testify/assert"
)

const MemQDBPath = ""

var mockDistribution = &qdb.Distribution{
	ID: "123",
}
var mockShard = &qdb.Shard{
	ID:    "shard_id",
	Hosts: []string{"host1", "host2"},
}
var mockKeyRange = &qdb.KeyRange{
	LowerBound: []byte{1, 2},
	ShardID:    mockShard.ID,
	KeyRangeID: "key_range_id",
}
var mockRouter = &qdb.Router{
	Address: "address",
	ID:      "router_id",
	State:   qdb.CLOSED,
}
var mockDataTransferTransaction = &qdb.DataTransferTransaction{
	ToShardId:   mockShard.ID,
	FromShardId: mockShard.ID,
	FromTxName:  "fake_tx_1",
	ToTxName:    "fake_tx_2",
	FromStatus:  "fake_st_1",
	ToStatus:    "fake_st_2",
}

// must run with -race
func TestMemqdbRacing(t *testing.T) {
	assert := assert.New(t)

	memqdb, err := qdb.RestoreQDB(MemQDBPath)
	assert.NoError(err)

	var wg sync.WaitGroup
	ctx := context.TODO()

	methods := []func(){
		func() { _ = memqdb.CreateDistribution(ctx, mockDistribution) },
		func() { _ = memqdb.AddKeyRange(ctx, mockKeyRange) },
		func() { _ = memqdb.AddRouter(ctx, mockRouter) },
		func() { _ = memqdb.AddShard(ctx, mockShard) },
		func() {
			_ = memqdb.RecordTransferTx(ctx, mockDataTransferTransaction.FromShardId, mockDataTransferTransaction)
		},
		func() { _, _ = memqdb.ListDistributions(ctx) },
		func() { _, _ = memqdb.ListAllKeyRanges(ctx) },
		func() { _, _ = memqdb.ListRouters(ctx) },
		func() { _, _ = memqdb.ListShards(ctx) },
		func() { _, _ = memqdb.GetKeyRange(ctx, mockKeyRange.KeyRangeID) },
		func() { _, _ = memqdb.GetShard(ctx, mockShard.ID) },
		func() { _, _ = memqdb.GetTransferTx(ctx, mockDataTransferTransaction.FromShardId) },
		func() { _ = memqdb.ShareKeyRange(mockKeyRange.KeyRangeID) },
		func() { _ = memqdb.DropKeyRange(ctx, mockKeyRange.KeyRangeID) },
		func() { _ = memqdb.DropKeyRangeAll(ctx) },
		func() { _ = memqdb.RemoveTransferTx(ctx, mockDataTransferTransaction.FromShardId) },
		func() {
			_, _ = memqdb.LockKeyRange(ctx, mockKeyRange.KeyRangeID)
			_ = memqdb.UnlockKeyRange(ctx, mockKeyRange.KeyRangeID)
		},
		func() { _ = memqdb.UpdateKeyRange(ctx, mockKeyRange) },
		func() { _ = memqdb.DeleteRouter(ctx, mockRouter.ID) },
	}
	for i := 0; i < 10; i++ {
		for _, m := range methods {
			wg.Add(1)
			go func(m func()) {
				m()
				wg.Done()
			}(m)
		}
		wg.Wait()

	}
	wg.Wait()
}

func TestDistributions(t *testing.T) {

	assert := assert.New(t)

	memqdb, err := qdb.RestoreQDB(MemQDBPath)
	assert.NoError(err)

	ctx := context.TODO()

	err = memqdb.CreateDistribution(ctx, qdb.NewDistribution("ds1", nil))

	assert.NoError(err)

	err = memqdb.CreateDistribution(ctx, qdb.NewDistribution("ds2", nil))

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

	ds, err := memqdb.GetRelationDistribution(ctx, relation.Name)
	assert.NoError(err)
	assert.Equal(ds.ID, "ds1")
	assert.Contains(ds.Relations, relation.Name)
	assert.Equal(ds.Relations[relation.Name], relation)

	assert.Error(memqdb.AlterDistributionAttach(ctx, "ds2", []*qdb.DistributedRelation{
		relation,
	}))

	assert.NoError(memqdb.AlterDistributionDetach(ctx, "ds1", "r1"))
	_, err = memqdb.GetRelationDistribution(ctx, relation.Name)
	assert.Error(err)

	ds, err = memqdb.GetDistribution(ctx, "ds1")
	assert.NoError(err)
	assert.NotContains(ds.Relations, relation.Name)

	assert.NoError(memqdb.AlterDistributionAttach(ctx, "ds2", []*qdb.DistributedRelation{
		relation,
	}))

	ds, err = memqdb.GetRelationDistribution(ctx, relation.Name)
	assert.NoError(err)
	assert.Equal(ds.ID, "ds2")
	assert.Contains(ds.Relations, relation.Name)
	assert.Equal(ds.Relations[relation.Name], relation)

	oldDs, err := memqdb.GetDistribution(ctx, "ds1")
	assert.NoError(err)
	assert.NotContains(oldDs.Relations, relation.Name)
}

func TestMemQDB_GetNotAttachedRelationDistribution(t *testing.T) {
	assert := assert.New(t)

	memQDB, err := qdb.RestoreQDB(MemQDBPath)
	assert.NoError(err)

	ctx := context.TODO()

	assert.NoError(memQDB.CreateDistribution(ctx, qdb.NewDistribution("ds1", nil)))

	_, err = memQDB.GetRelationDistribution(ctx, "rel")
	assert.Error(err)
}

func TestMemQDB_GetAbsentDistribution(t *testing.T) {
	assert := assert.New(t)

	memQDB, err := qdb.RestoreQDB(MemQDBPath)
	assert.NoError(err)

	ctx := context.TODO()

	_, err = memQDB.GetDistribution(ctx, "absent")
	assert.Error(err)
}

func TestKeyRanges(t *testing.T) {

	assert := assert.New(t)

	memqdb, err := qdb.RestoreQDB(MemQDBPath)
	assert.NoError(err)

	ctx := context.TODO()

	err = memqdb.CreateDistribution(ctx, qdb.NewDistribution("ds1", nil))

	assert.NoError(err)

	err = memqdb.CreateDistribution(ctx, qdb.NewDistribution("ds2", nil))

	assert.NoError(err)

	assert.NoError(memqdb.AddKeyRange(ctx, &qdb.KeyRange{
		LowerBound:     []byte("1111"),
		ShardID:        "sh1",
		KeyRangeID:     "krid1",
		DistributionId: "ds1",
	}))

	assert.Error(memqdb.AddKeyRange(ctx, &qdb.KeyRange{
		LowerBound:     []byte("1111"),
		ShardID:        "sh1",
		KeyRangeID:     "krid2",
		DistributionId: "dserr",
	}))

}
