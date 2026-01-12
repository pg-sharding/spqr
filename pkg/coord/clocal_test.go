package coord_test

import (
	"context"
	"testing"

	"github.com/pg-sharding/spqr/pkg/coord"
	"github.com/pg-sharding/spqr/qdb"
	mock "github.com/pg-sharding/spqr/qdb/mock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestListKeyRangesCaches(t *testing.T) {

	assert := assert.New(t)
	ctrl := gomock.NewController(t)

	db := mock.NewMockXQDB(ctrl)

	lc := coord.NewLocalInstanceMetadataMgr(db, nil, nil)

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
