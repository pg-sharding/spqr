package qdb_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/rfqn"
	"github.com/stretchr/testify/assert"
)

const MemQDBPath = ""

var mockDistribution = &qdb.Distribution{
	ID: "123",
}
var mockShard = &qdb.Shard{
	ID:       "shard_id",
	RawHosts: []string{"host1", "host2"},
}
var mockKeyRange = &qdb.KeyRange{
	LowerBound: [][]byte{{1, 2}},
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
	Status:      "fake_st",
}

// must run with -race
func TestMemqdbRacing(t *testing.T) {
	assert := assert.New(t)

	memqdb, err := qdb.RestoreQDB(MemQDBPath)
	assert.NoError(err)

	var wg sync.WaitGroup
	ctx := context.TODO()

	methods := []func(){
		func() {
			if stmts, err := memqdb.CreateDistribution(ctx, mockDistribution); err != nil {
				panic("fail run CreateDistribution in race test (prepare phase)")
			} else {
				if err = memqdb.ExecNoTransaction(ctx, stmts); err != nil {
					panic("fail run CreateDistribution in race test (exec phase)")
				}
			}

		},
		func() { _ = memqdb.CreateKeyRange(ctx, mockKeyRange) },
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
		func() {
			tran1, err := qdb.NewTransaction()
			if err != nil {
				panic("cann't create transaction structure (begin transaction test)!")
			}
			_ = memqdb.BeginTransaction(ctx, tran1)
		},
		func() {
			dataDistribution1, err := json.Marshal(mockDistribution)
			if err != nil {
				panic("cann't unmarshal distribution (exec no transaction test)!")
			}
			commands := []qdb.QdbStatement{
				{CmdType: qdb.CMD_PUT, Key: mockDistribution.ID, Value: string(dataDistribution1), Extension: qdb.MapDistributions},
			}
			_ = memqdb.ExecNoTransaction(ctx, commands)
		},
		func() {
			tran, err := qdb.NewTransaction()
			if err != nil {
				panic("can't create transaction structure (commit transaction test)!")
			}
			dataDistribution1, err := json.Marshal(mockDistribution)
			if err != nil {
				panic("cann't unmarshal distribution case(1)!")
			}
			commands := []qdb.QdbStatement{
				{CmdType: qdb.CMD_PUT, Key: mockDistribution.ID, Value: string(dataDistribution1), Extension: qdb.MapDistributions},
			}
			err = tran.Append(commands)
			if err != nil {
				panic("fail append commands to transaction!")
			}
			err = memqdb.BeginTransaction(ctx, tran)
			if err != nil {
				panic("cann't begin transaction!")
			}
			_ = memqdb.ExecNoTransaction(ctx, commands)
		},
	}
	for range 1000 {
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

	chunk, err := memqdb.CreateDistribution(ctx, qdb.NewDistribution("ds1", nil))
	assert.NoError(err)
	err = memqdb.ExecNoTransaction(ctx, chunk)
	assert.NoError(err)

	chunk, err = memqdb.CreateDistribution(ctx, qdb.NewDistribution("ds2", nil))
	assert.NoError(err)
	err = memqdb.ExecNoTransaction(ctx, chunk)
	assert.NoError(err)

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
	qualifiedName := relation.QualifiedName()
	ds, err := memqdb.GetRelationDistribution(ctx, qualifiedName)
	assert.NoError(err)
	assert.Equal(ds.ID, "ds1")
	assert.Contains(ds.Relations, relation.Name)
	assert.Equal(ds.Relations[relation.Name], relation)

	assert.Error(memqdb.AlterDistributionAttach(ctx, "ds2", []*qdb.DistributedRelation{
		relation,
	}))

	assert.NoError(memqdb.AlterDistributionDetach(ctx, "ds1", &rfqn.RelationFQN{RelationName: "r1"}))
	_, err = memqdb.GetRelationDistribution(ctx, qualifiedName)
	assert.Error(err)

	ds, err = memqdb.GetDistribution(ctx, "ds1")
	assert.NoError(err)
	assert.NotContains(ds.Relations, relation.Name)

	assert.NoError(memqdb.AlterDistributionAttach(ctx, "ds2", []*qdb.DistributedRelation{
		relation,
	}))

	ds, err = memqdb.GetRelationDistribution(ctx, qualifiedName)
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

	chunk, err := memQDB.CreateDistribution(ctx, qdb.NewDistribution("ds1", nil))
	assert.NoError(err)
	err = memQDB.ExecNoTransaction(ctx, chunk)
	assert.NoError(err)

	_, err = memQDB.GetRelationDistribution(ctx, &rfqn.RelationFQN{RelationName: "rel"})
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

func TestDropReferenceRelation(t *testing.T) {

	assert := assert.New(t)

	memqdb, err := qdb.RestoreQDB(MemQDBPath)
	assert.NoError(err)

	ctx := context.TODO()
	referenceRelation := &qdb.ReferenceRelation{
		TableName:             "test2",
		SchemaVersion:         1,
		ColumnSequenceMapping: map[string]string{"id": "test2_id"},
		ShardIds:              []string{"sh1", "sh2"},
	}
	err = memqdb.CreateReferenceRelation(ctx, referenceRelation)
	assert.NoError(err)
	err = memqdb.DropReferenceRelation(ctx, &rfqn.RelationFQN{RelationName: "test2"})
	assert.NoError(err)
	_, err = memqdb.GetReferenceRelation(ctx, &rfqn.RelationFQN{RelationName: "test2"})
	assert.Error(err)
	assert.Equal(0, len(memqdb.Sequences))
	assert.Equal(0, len(memqdb.ColumnSequence))
	assert.Equal(0, len(memqdb.SequenceToValues))
	assert.Equal(0, len(memqdb.ReferenceRelations))

	assert.NoError(memqdb.DropKeyRange(ctx, "nonexistentKeyRange"))
}

func TestKeyRanges(t *testing.T) {

	assert := assert.New(t)

	memqdb, err := qdb.RestoreQDB(MemQDBPath)
	assert.NoError(err)

	ctx := context.TODO()

	chunk, err := memqdb.CreateDistribution(ctx, qdb.NewDistribution("ds1", nil))
	assert.NoError(err)
	memqdb.ExecNoTransaction(ctx, chunk)
	assert.NoError(err)

	chunk, err = memqdb.CreateDistribution(ctx, qdb.NewDistribution("ds2", nil))
	assert.NoError(err)
	memqdb.ExecNoTransaction(ctx, chunk)
	assert.NoError(err)

	assert.NoError(err)

	assert.NoError(memqdb.CreateKeyRange(ctx, &qdb.KeyRange{
		LowerBound:     [][]byte{[]byte("1111")},
		ShardID:        "sh1",
		KeyRangeID:     "krid1",
		DistributionId: "ds1",
	}))

	assert.Error(memqdb.CreateKeyRange(ctx, &qdb.KeyRange{
		LowerBound:     [][]byte{[]byte("1111")},
		ShardID:        "sh1",
		KeyRangeID:     "krid2",
		DistributionId: "dserr",
	}))

	assert.NoError(memqdb.DropKeyRange(ctx, "nonexistentKeyRange"))
}

func Test_MemQDB_GetKeyRange(t *testing.T) {

	assert := assert.New(t)

	memqdb, err := qdb.RestoreQDB(MemQDBPath)
	assert.NoError(err)

	ctx := context.TODO()

	chunk, err := memqdb.CreateDistribution(ctx, qdb.NewDistribution("ds1", nil))
	assert.NoError(err)
	memqdb.ExecNoTransaction(ctx, chunk)
	assert.NoError(err)

	chunk, err = memqdb.CreateDistribution(ctx, qdb.NewDistribution("ds2", nil))
	assert.NoError(err)
	memqdb.ExecNoTransaction(ctx, chunk)
	assert.NoError(err)

	keyRange1 := qdb.KeyRange{
		LowerBound:     [][]byte{[]byte("1111")},
		ShardID:        "sh1",
		KeyRangeID:     "krid1",
		DistributionId: "ds1",
	}
	assert.NoError(memqdb.CreateKeyRange(ctx, &keyRange1))

	keyRange2 := qdb.KeyRange{
		LowerBound:     [][]byte{[]byte("1111")},
		ShardID:        "sh1",
		KeyRangeID:     "krid2",
		DistributionId: "ds2",
	}
	assert.NoError(memqdb.CreateKeyRange(ctx, &keyRange2))

	res, _ := memqdb.GetKeyRange(ctx, keyRange1.KeyRangeID)
	assert.Equal(keyRange1.ShardID, res.ShardID)
	assert.Equal(keyRange1.KeyRangeID, res.KeyRangeID)
	assert.Equal(keyRange1.DistributionId, res.DistributionId)
	assert.Equal(keyRange1.LowerBound, res.LowerBound)

	_, err = memqdb.GetKeyRange(ctx, "krid3")
	assert.NotNil(err)
}

func TestMemQDB_NextVal(t *testing.T) {
	assert := assert.New(t)
	ctx := context.TODO()

	memqdb, err := qdb.NewMemQDB("")
	assert.NoError(err)

	err = memqdb.CreateSequence(ctx, "seq", 0)
	assert.NoError(err)

	err = memqdb.AlterSequenceAttach(ctx, "seq", &rfqn.RelationFQN{RelationName: "test"}, "id")
	assert.NoError(err)

	// Test concurrency
	var wg sync.WaitGroup
	const goroutines = 10
	const increments = 1000
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < increments; j++ {
				_, err := memqdb.NextRange(ctx, "seq", 1)
				assert.NoError(err)
			}
		}()
	}

	wg.Wait()

	// Verify final value
	expectedValue := int64(goroutines*increments + 1)
	idRange, err := memqdb.NextRange(ctx, "seq", 1)
	assert.NoError(err)
	assert.Equal(expectedValue, idRange.Right)
}
