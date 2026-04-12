package coord

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/stretchr/testify/assert"
)

const MemQDBPath = ""

var boolFalse bool = false

var mockShard1 = &qdb.Shard{
	ID:       "sh1",
	RawHosts: []string{"host1", "host2"},
}
var mockShard2 = &qdb.Shard{
	ID:       "sh2",
	RawHosts: []string{"host3", "host4"},
}

func prepareDB(ctx context.Context) (*qdb.MemQDB, error) {
	memqdb, err := qdb.RestoreQDB(MemQDBPath)
	if err != nil {
		return nil, err
	}
	var chunk []qdb.QdbStatement
	if chunk, err = memqdb.CreateDistribution(ctx, qdb.NewDistribution("ds1", nil)); err != nil {
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

func TestSplitKeyRange(t *testing.T) {

	t.Run("split fail locked", func(t *testing.T) {
		is := assert.New(t)
		ctx := context.Background()
		memqdb, err := prepareDB(ctx)
		is.NoError(err)
		mngr := NewLocalInstanceMetadataMgr(memqdb, nil, nil, map[string]*topology.DataShard{}, false, nil)
		tranMngr := meta.NewTranEntityManager(mngr)

		ds1 := distributions.NewDistribution("ds1", []string{"integer"})
		err = tranMngr.CreateDistribution(ctx, ds1)
		is.NoError(err)
		var kr1 = &kr.KeyRange{
			ID:           "kr1",
			ShardID:      "sh1",
			Distribution: "ds1",
			LowerBound:   []any{int64(1)},
			ColumnTypes:  []string{qdb.ColumnTypeInteger},
			IsLocked:     &boolFalse,
		}
		err = tranMngr.CreateKeyRange(ctx, kr1, ds1.ColTypes)
		is.NoError(err)
		err = tranMngr.ExecNoTran(ctx)
		is.NoError(err)
		_, err = mngr.LockKeyRange(ctx, kr1.ID)
		is.NoError(err)

		buf := make([]byte, binary.MaxVarintLen64)
		binary.PutVarint(buf, int64(1))
		splitData := &kr.SplitKeyRange{
			Bound:     [][]byte{buf},
			SourceID:  "kr1",
			Krid:      "kr2",
			SplitLeft: true,
		}

		err = mngr.Split(ctx, splitData)
		is.EqualError(err, "key range kr1 is locked")
	})
	t.Run("split happy path", func(t *testing.T) {
		is := assert.New(t)
		ctx := context.Background()
		memqdb, err := prepareDB(ctx)
		is.NoError(err)
		mngr := NewLocalInstanceMetadataMgr(memqdb, nil, nil, map[string]*topology.DataShard{}, false, nil)
		tranMngr := meta.NewTranEntityManager(mngr)

		ds1 := distributions.NewDistribution("ds1", []string{"integer"})
		err = tranMngr.CreateDistribution(ctx, ds1)
		is.NoError(err)
		var kr1 = &kr.KeyRange{
			ID:           "kr1",
			ShardID:      "sh1",
			Distribution: "ds1",
			LowerBound:   []any{int64(1)},
			ColumnTypes:  []string{qdb.ColumnTypeInteger},
			IsLocked:     &boolFalse,
			Version:      1,
		}
		err = tranMngr.CreateKeyRange(ctx, kr1, []string{qdb.ColumnTypeInteger})
		is.NoError(err)
		err = tranMngr.ExecNoTran(ctx)
		is.NoError(err)

		buf := make([]byte, binary.MaxVarintLen64)
		binary.PutVarint(buf, int64(5))
		splitData := &kr.SplitKeyRange{
			Bound:     [][]byte{buf},
			SourceID:  "kr1",
			Krid:      "kr2",
			SplitLeft: false,
		}

		err = mngr.Split(ctx, splitData)
		is.NoError(err)
		actual, err := mngr.ListAllKeyRanges(ctx)
		is.NoError(err)
		expected := []kr.KeyRange{
			*kr1,
			{
				ID:           "kr2",
				ShardID:      "sh1",
				Distribution: "ds1",
				LowerBound:   []any{int64(5)},
				ColumnTypes:  []string{qdb.ColumnTypeInteger},
				IsLocked:     &boolFalse,
				Version:      1,
			},
		}
		// IsLocked is not values of bool :(
		is.Equal([]bool{false, false}, []bool{*actual[0].IsLocked, *actual[0].IsLocked})
		actual[0].IsLocked = &boolFalse
		actual[1].IsLocked = &boolFalse
		is.Equal(expected, []kr.KeyRange{*actual[0], *actual[1]})
	})
}
