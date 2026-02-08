package meta_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	mtran "github.com/pg-sharding/spqr/pkg/models/transaction"
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/stretchr/testify/assert"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/coord"
	"github.com/pg-sharding/spqr/pkg/meta"
	distributions "github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/qdb"
)

func TestTranEntitySaveBefore(t *testing.T) {
	is := assert.New(t)
	list := meta.NewMetaEntityList[*distributions.Distribution]()
	list.Save("ds1", &distributions.Distribution{Id: "ds1", ColTypes: []string{"integer"}})
	list.Save("ds2", &distributions.Distribution{Id: "ds2", ColTypes: []string{"integer"}})
	expectedExist := map[string]*distributions.Distribution{"ds1": {Id: "ds1", ColTypes: []string{"integer"}}, "ds2": {Id: "ds2", ColTypes: []string{"integer"}}}
	expectedDeleted := map[string]struct{}{}
	is.Equal(expectedExist, list.Items())
	is.Equal(expectedDeleted, list.DeletedItems())

	list.Delete("ds1")
	expectedExist = map[string]*distributions.Distribution{"ds2": {Id: "ds2", ColTypes: []string{"integer"}}}
	expectedDeleted = map[string]struct{}{"ds1": {}}
	is.Equal(expectedExist, list.Items())
	is.Equal(expectedDeleted, list.DeletedItems())
}
func TestTranEntityDelBefore(t *testing.T) {
	is := assert.New(t)
	list := meta.NewMetaEntityList[*distributions.Distribution]()
	list.Delete("ds1")
	list.Delete("ds3")
	expectedExist := map[string]*distributions.Distribution{}
	expectedDeleted := map[string]struct{}{"ds1": {}, "ds3": {}}
	is.Equal(expectedExist, list.Items())
	is.Equal(expectedDeleted, list.DeletedItems())

	list.Save("ds3", &distributions.Distribution{Id: "ds3", ColTypes: []string{"integer"}})
	expectedExist = map[string]*distributions.Distribution{"ds3": {Id: "ds3", ColTypes: []string{"integer"}}}
	expectedDeleted = map[string]struct{}{"ds1": {}}
	is.Equal(expectedExist, list.Items())
	is.Equal(expectedDeleted, list.DeletedItems())
}

func TestTranGetDistribution(t *testing.T) {
	is := assert.New(t)

	t.Run("test with save changes", func(t *testing.T) {
		ctx := context.Background()
		memqdb, err := prepareDB(ctx)
		assert.NoError(t, err)
		mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil, map[string]*config.Shard{}, false)
		ds0 := distributions.NewDistribution("ds0", []string{"integer"})
		statements, err := mngr.CreateDistribution(ctx, ds0)
		is.NoError(err)
		err = memqdb.ExecNoTransaction(ctx, statements)
		is.NoError(err)

		tranMngr := meta.NewTranEntityManager(mngr)

		ds1 := distributions.NewDistribution("ds1", []string{"integer"})
		ds2 := distributions.NewDistribution("ds2", []string{"integer"})
		err = tranMngr.CreateDistribution(ctx, ds1)
		is.NoError(err)
		//NO COMMIT QDB!!!
		err = tranMngr.CreateDistribution(ctx, ds2)
		is.NoError(err)
		//NO COMMIT QDB!!!

		//check List
		actualList, err := tranMngr.ListDistributions(ctx)
		is.NoError(err)
		is.Len(actualList, 3)
		is.Equal(map[string]*distributions.Distribution{"ds1": ds1, "ds2": ds2, "ds0": ds0},
			map[string]*distributions.Distribution{actualList[0].Id: actualList[0], actualList[1].Id: actualList[1], actualList[2].Id: actualList[2]})

		//check Get
		actual1, err := tranMngr.GetDistribution(ctx, "ds1")
		is.NoError(err)
		is.Equal(ds1, actual1)
		actual0, err := tranMngr.GetDistribution(ctx, "ds0")
		is.NoError(err)
		is.Equal(ds0, actual0)
		_, err = tranMngr.GetDistribution(ctx, "ds-1")
		is.EqualError(err, "distribution \"ds-1\" not found")
	})

	t.Run("test with delete changes", func(t *testing.T) {
		ctx := context.Background()
		memqdb, err := prepareDB(ctx)
		assert.NoError(t, err)
		mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil, map[string]*config.Shard{}, false)
		ds0 := distributions.NewDistribution("ds0", []string{"integer"})
		statements, err := mngr.CreateDistribution(ctx, ds0)
		is.NoError(err)
		err = memqdb.ExecNoTransaction(ctx, statements)
		is.NoError(err)

		tranMngr := meta.NewTranEntityManager(mngr)

		ds1 := distributions.NewDistribution("ds1", []string{"integer"})
		ds2 := distributions.NewDistribution("ds2", []string{"integer"})
		err = tranMngr.CreateDistribution(ctx, ds1)
		is.NoError(err)
		//NO COMMIT QDB!!!
		err = tranMngr.CreateDistribution(ctx, ds2)
		is.NoError(err)
		//NO COMMIT QDB!!!
		err = tranMngr.DropDistribution(ctx, "ds2")
		is.NoError(err)
		//NO COMMIT QDB!!!
		err = tranMngr.DropDistribution(ctx, "ds0")
		is.NoError(err)
		//NO COMMIT QDB!!!

		//check List
		actualList, err := tranMngr.ListDistributions(ctx)
		is.NoError(err)
		is.Equal([]*distributions.Distribution{ds1}, actualList)

		//check Get
		_, err = tranMngr.GetDistribution(ctx, "ds2")
		is.EqualError(err, "distribution \"ds2\" not found")
		_, err = tranMngr.GetDistribution(ctx, "ds0")
		is.EqualError(err, "distribution \"ds0\" not found")

		actualQdb, err := mngr.GetDistribution(ctx, "ds0")
		is.NoError(err)
		is.Equal(ds0, actualQdb)
	})
}
func TestTranGetKeyRange(t *testing.T) {
	is := assert.New(t)
	t.Run("test with save changes", func(t *testing.T) {
		ctx := context.Background()
		memqdb, err := prepareDbTestValidate(ctx)
		is.NoError(err)
		mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil, map[string]*config.Shard{}, false)
		var kr1 = &kr.KeyRange{
			ID:           "kr1",
			ShardID:      "sh1",
			Distribution: "ds1",
			LowerBound:   []any{int64(0)},
			ColumnTypes:  []string{qdb.ColumnTypeInteger},
			IsLocked:     &boolTrue,
		}
		statements, err := mngr.CreateKeyRange(ctx, kr1)
		is.NoError(err)
		err = memqdb.ExecNoTransaction(ctx, statements)
		is.NoError(err)

		tranMngr := meta.NewTranEntityManager(mngr)

		var kr2 = &kr.KeyRange{
			ID:           "kr2",
			ShardID:      "sh1",
			Distribution: "ds1",
			LowerBound:   []any{int64(10)},
			ColumnTypes:  []string{qdb.ColumnTypeInteger},
			IsLocked:     &boolTrue,
		}

		var kr2Ds2 = &kr.KeyRange{
			ID:           "kr2_ds2",
			ShardID:      "sh1",
			Distribution: "ds2",
			LowerBound:   []any{int64(10)},
			ColumnTypes:  []string{qdb.ColumnTypeInteger},
			IsLocked:     &boolTrue,
		}
		var kr2Double = &kr.KeyRange{
			ID:           "kr2",
			ShardID:      "sh1",
			Distribution: "ds2",
			LowerBound:   []any{int64(10)},
			ColumnTypes:  []string{qdb.ColumnTypeInteger},
			IsLocked:     &boolFalse,
		}

		err = meta.ValidateKeyRangeForCreate(ctx, tranMngr, kr1)
		is.Error(err)
		is.EqualError(err, "key range kr1 already present in qdb")
		err = meta.ValidateKeyRangeForCreate(ctx, tranMngr, kr2)
		is.NoError(err)
		err = tranMngr.CreateKeyRange(ctx, kr2)
		//NO COMMIT QDB!!!
		is.NoError(err)
		err = tranMngr.CreateKeyRange(ctx, kr2Ds2)
		//NO COMMIT QDB!!!
		is.NoError(err)
		err = tranMngr.CreateKeyRange(ctx, kr2Double)
		//NO COMMIT QDB!!!
		is.EqualError(err, "key range kr2 already present in qdb")

		//check List
		actualList, err := tranMngr.ListKeyRanges(ctx, "ds1")
		is.NoError(err)
		is.Equal([]*kr.KeyRange{kr2, kr1}, actualList)
		actualList, err = tranMngr.ListKeyRanges(ctx, "ds2")
		is.NoError(err)
		is.Equal([]*kr.KeyRange{kr2Ds2}, actualList)

		//check Get
		_, err = tranMngr.GetKeyRange(ctx, "kr1DOUBLE")
		is.EqualError(err, "there is no key range kr1DOUBLE")
		_, err = tranMngr.GetKeyRange(ctx, "kr1")
		is.NoError(err)
		_, err = tranMngr.GetKeyRange(ctx, "kr2")
		is.NoError(err)

	})
}

func TestTranState(t *testing.T) {
	t.Run("setTransaction happy path", func(t *testing.T) {
		is := assert.New(t)
		distribution := &proto.Distribution{
			Id:          "ds1",
			ColumnTypes: []string{"integer"},
		}
		transactionId1, err := uuid.Parse("6ca41a0b-8446-4098-abcf-d9802bea3447")
		is.NoError(err)
		chunk := mtran.NewMetaTransactionChunk([]*proto.MetaTransactionGossipCommand{
			{
				CreateDistribution: &proto.CreateDistributionGossip{
					Distributions: []*proto.Distribution{distribution},
				},
			},
		},
		)
		is.NoError(err)
		tran1 := mtran.MetaTransaction{
			TransactionId: transactionId1,
			Operations:    chunk,
		}

		state := meta.NewTransactionState()
		err = state.SetTransaction(&tran1)
		is.NoError(err)
	})
	t.Run("setTransaction tran over tran fails", func(t *testing.T) {
		is := assert.New(t)
		distribution := &proto.Distribution{
			Id:          "ds1",
			ColumnTypes: []string{"integer"},
		}
		transactionId1, err := uuid.Parse("6ca41a0b-8446-4098-abcf-d9802bea3447")
		is.NoError(err)
		transactionId2, err := uuid.Parse("7ca41a0b-8446-4098-abcf-d9802bea3447")
		is.NoError(err)
		chunk := mtran.NewMetaTransactionChunk([]*proto.MetaTransactionGossipCommand{
			{
				CreateDistribution: &proto.CreateDistributionGossip{
					Distributions: []*proto.Distribution{distribution},
				},
			},
		},
		)
		is.NoError(err)
		tran1 := mtran.MetaTransaction{
			TransactionId: transactionId1,
			Operations:    chunk,
		}
		tran2 := mtran.MetaTransaction{
			TransactionId: transactionId2,
			Operations:    chunk,
		}

		state := meta.NewTransactionState()
		err = state.SetTransaction(&tran1)
		is.NoError(err)
		err = state.SetTransaction(&tran2)
		is.EqualError(err, "corrupted transaction state (setTransaction)")
	})
	t.Run("setTransaction tran over chunk fails", func(t *testing.T) {
		is := assert.New(t)
		distribution := &proto.Distribution{
			Id:          "ds1",
			ColumnTypes: []string{"integer"},
		}
		transactionId2, err := uuid.Parse("7ca41a0b-8446-4098-abcf-d9802bea3447")
		is.NoError(err)
		chunk := mtran.NewMetaTransactionChunk([]*proto.MetaTransactionGossipCommand{
			{
				CreateDistribution: &proto.CreateDistributionGossip{
					Distributions: []*proto.Distribution{distribution},
				},
			},
		},
		)
		is.NoError(err)
		tran2 := mtran.MetaTransaction{
			TransactionId: transactionId2,
			Operations:    chunk,
		}

		state := meta.NewTransactionState()
		err = state.Append([]*proto.MetaTransactionGossipCommand{
			{
				CreateDistribution: &proto.CreateDistributionGossip{
					Distributions: []*proto.Distribution{distribution},
				},
			},
		})
		is.NoError(err)
		err = state.SetTransaction(&tran2)
		is.EqualError(err, "transaction state begins with no transaction flow")
	})
	t.Run("append chunk for tran success", func(t *testing.T) {
		is := assert.New(t)
		distribution := &proto.Distribution{
			Id:          "ds1",
			ColumnTypes: []string{"integer"},
		}
		transactionId2, err := uuid.Parse("7ca41a0b-8446-4098-abcf-d9802bea3447")
		is.NoError(err)
		chunk := mtran.NewMetaTransactionChunk([]*proto.MetaTransactionGossipCommand{
			{
				CreateDistribution: &proto.CreateDistributionGossip{
					Distributions: []*proto.Distribution{distribution},
				},
			},
		},
		)
		is.NoError(err)
		tran2 := mtran.MetaTransaction{
			TransactionId: transactionId2,
			Operations:    chunk,
		}

		state := meta.NewTransactionState()

		err = state.SetTransaction(&tran2)
		is.NoError(err)

		err = state.Append([]*proto.MetaTransactionGossipCommand{
			{
				CreateDistribution: &proto.CreateDistributionGossip{
					Distributions: []*proto.Distribution{distribution},
				},
			},
		})

		is.NoError(err)
	})

	t.Run("append chunk for chunk success", func(t *testing.T) {
		is := assert.New(t)
		distribution1 := &proto.Distribution{Id: "ds1", ColumnTypes: []string{"integer"}}
		distribution2 := &proto.Distribution{Id: "ds2", ColumnTypes: []string{"integer"}}

		chunk1 := mtran.NewMetaTransactionChunk([]*proto.MetaTransactionGossipCommand{
			{
				CreateDistribution: &proto.CreateDistributionGossip{
					Distributions: []*proto.Distribution{distribution1},
				},
			},
		},
		)
		chunk2 := mtran.NewMetaTransactionChunk([]*proto.MetaTransactionGossipCommand{
			{
				CreateDistribution: &proto.CreateDistributionGossip{
					Distributions: []*proto.Distribution{distribution2},
				},
			},
		},
		)

		state := meta.NewTransactionState()

		err := state.Append(chunk1.GossipRequests)
		is.NoError(err)
		err = state.Append(chunk2.GossipRequests)
		is.NoError(err)
		expected := mtran.NewMetaTransactionChunk([]*proto.MetaTransactionGossipCommand{
			{
				CreateDistribution: &proto.CreateDistributionGossip{
					Distributions: []*proto.Distribution{distribution1},
				},
			},
			{
				CreateDistribution: &proto.CreateDistributionGossip{
					Distributions: []*proto.Distribution{distribution2},
				},
			},
		},
		)
		is.Equal(state.Chunk, expected)
	})

	t.Run("test double execute chunk fails", func(t *testing.T) {
		is := assert.New(t)
		ctx := context.Background()
		memqdb, err := prepareDB(ctx)
		assert.NoError(t, err)
		mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil, map[string]*config.Shard{}, false)
		tranMngr := meta.NewTranEntityManager(mngr)

		ds1 := distributions.NewDistribution("ds1", []string{"integer"})
		err = tranMngr.CreateDistribution(ctx, ds1)
		is.NoError(err)
		err = tranMngr.ExecNoTran(ctx)
		is.NoError(err)
		err = tranMngr.ExecNoTran(ctx)
		is.EqualError(err, "can't double execute chunk")
	})
	t.Run("test double commit transaction fails", func(t *testing.T) {
		is := assert.New(t)
		ctx := context.Background()
		memqdb, err := prepareDB(ctx)
		assert.NoError(t, err)
		mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil, map[string]*config.Shard{}, false)
		tranMngr := meta.NewTranEntityManager(mngr)
		err = tranMngr.BeginTran(ctx)
		is.NoError(err)
		ds1 := distributions.NewDistribution("ds1", []string{"integer"})
		err = tranMngr.CreateDistribution(ctx, ds1)
		is.NoError(err)
		err = tranMngr.CommitTran(ctx)
		is.NoError(err)
		err = tranMngr.CommitTran(ctx)
		is.EqualError(err, "can't double transaction")
	})
}
