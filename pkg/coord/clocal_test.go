package coord_test

import (
	"context"
	"testing"

	"github.com/pg-sharding/spqr/pkg/coord"
	shardmock "github.com/pg-sharding/spqr/pkg/mock/shard"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/qdb"
	mock "github.com/pg-sharding/spqr/qdb/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestListKeyRangesCaches(t *testing.T) {

	assert := assert.New(t)
	ctrl := gomock.NewController(t)

	db := mock.NewMockXQDB(ctrl)

	lc := coord.NewLocalInstanceMetadataMgr(db, nil, nil, map[string]*topology.DataShard{}, false, nil)

	krs := []*qdb.KeyRange{
		{
			LowerBound:     [][]byte{[]byte("1")},
			ShardID:        "sh1",
			KeyRangeID:     "kr1",
			DistributionId: "ds1",
		},

		{
			LowerBound:     [][]byte{[]byte("2")},
			ShardID:        "sh2",
			KeyRangeID:     "kr2",
			DistributionId: "ds1",
		},

		{
			LowerBound:     [][]byte{[]byte("3")},
			ShardID:        "sh3",
			KeyRangeID:     "kr3",
			DistributionId: "ds1",
		},

		{
			LowerBound:     [][]byte{[]byte("4")},
			ShardID:        "sh4",
			KeyRangeID:     "kr4",
			DistributionId: "ds2",
		},

		{
			LowerBound:     [][]byte{[]byte("5")},
			ShardID:        "sh5",
			KeyRangeID:     "kr5",
			DistributionId: "ds2",
		},
	}
	db.EXPECT().ListAllKeyRanges(gomock.All()).Return(krs, nil)
	/* check that request caches */

	ds1 := &qdb.Distribution{
		ID: "ds1",
		ColTypes: []string{
			qdb.ColumnTypeVarchar,
		},
	}

	ds2 := &qdb.Distribution{
		ID: "ds2",
		ColTypes: []string{
			qdb.ColumnTypeVarchar,
		},
	}

	db.EXPECT().GetDistribution(gomock.Any(), "ds1").Times(1).Return(ds1, nil)
	db.EXPECT().GetDistribution(gomock.Any(), "ds2").Times(1).Return(ds2, nil)

	_, err := lc.ListAllKeyRanges(context.Background())

	assert.NoError(err)
}

func TestLocalInstanceMetadataMgr_UpdateShard_invalidatesMatchingPoolHosts(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)

	db, err := qdb.NewMemQDB("")
	require.NoError(t, err)
	require.NoError(t, db.AddShard(ctx, &qdb.Shard{ID: "sh1", RawHosts: []string{"h1:5432"}}))

	iter := shardmock.NewMockShardHostIterator(ctrl)
	shCtl := shardmock.NewMockShardHostCtl(ctrl)
	iter.EXPECT().ForEach(gomock.Any()).DoAndReturn(func(cb func(sh shard.ShardHostCtl) error) error {
		return cb(shCtl)
	})
	shCtl.EXPECT().ShardKeyName().Return("sh1")
	shCtl.EXPECT().MarkStale()

	mgr := coord.NewLocalInstanceMetadataMgr(db, db, nil, nil, false, iter)
	err = mgr.AlterShardOptions(ctx, "sh1", []topology.GenericOption{
		{Name: "HOST", Arg: "host1:6432", Action: topology.GenericOptionActionAdd},
	})
	require.NoError(t, err)
}

func TestLocalInstanceMetadataMgr_UpdateShard_skipsStaleForNonMatchingShardKey(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)

	db, err := qdb.NewMemQDB("")
	require.NoError(t, err)
	require.NoError(t, db.AddShard(ctx, &qdb.Shard{ID: "sh1", RawHosts: []string{"h1:5432"}}))

	iter := shardmock.NewMockShardHostIterator(ctrl)
	shCtl := shardmock.NewMockShardHostCtl(ctrl)
	iter.EXPECT().ForEach(gomock.Any()).DoAndReturn(func(cb func(sh shard.ShardHostCtl) error) error {
		return cb(shCtl)
	})
	shCtl.EXPECT().ShardKeyName().Return("other")

	mgr := coord.NewLocalInstanceMetadataMgr(db, db, nil, nil, false, iter)
	err = mgr.AlterShardOptions(ctx, "sh1", []topology.GenericOption{
		{Name: "HOST", Arg: "host1:6432", Action: topology.GenericOptionActionAdd},
	})
	require.NoError(t, err)
}
