package rmeta

import (
	"context"
	"fmt"
	"testing"

	"github.com/pg-sharding/spqr/pkg/coord"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/qdb"
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

func prepareDB(ctx context.Context) (*qdb.MemQDB, error) {
	memqdb, err := qdb.RestoreQDB(MemQDBPath)
	if err != nil {
		return nil, err
	}
	d1 := &distributions.Distribution{Id: "ds1", ColTypes: []string{"integer"}}
	if err = memqdb.CreateDistribution(ctx, distributions.DistributionToDB(d1)); err != nil {
		return nil, err
	}
	d2 := &distributions.Distribution{Id: "ds_hashed", ColTypes: []string{"uinteger"}}
	if err = memqdb.CreateDistribution(ctx, distributions.DistributionToDB(d2)); err != nil {
		return nil, err
	}
	d3 := &distributions.Distribution{Id: "ds_2keys", ColTypes: []string{"integer", "varchar"}}
	if err = memqdb.CreateDistribution(ctx, distributions.DistributionToDB(d3)); err != nil {
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

func TestAlterDistributionAttach(t *testing.T) {
	ctx := context.Background()
	is := assert.New(t)
	for _, testData := range []struct {
		dsId string
		rel  []*distributions.DistributedRelation
		err  error
	}{
		{
			dsId: "ds_hashed",
			rel: []*distributions.DistributedRelation{
				{
					Name: "rel",
					DistributionKey: []distributions.DistributionKeyEntry{
						{Column: "col1", HashFunction: "ident"},
					},
				},
			},
			err: fmt.Errorf("automatic attach isn't supported for column key uinteger"),
		},
		{
			dsId: "ds1",
			rel: []*distributions.DistributedRelation{
				{
					Name: "rel",
					DistributionKey: []distributions.DistributionKeyEntry{
						{Column: "col1"},
					},
				},
			},
			err: nil,
		},
		{
			dsId: "ds_2keys",
			rel: []*distributions.DistributedRelation{
				{
					Name: "rel",
					DistributionKey: []distributions.DistributionKeyEntry{
						{Column: "col1"},
					},
				},
			},
			err: fmt.Errorf("automatic attach is supported only for distribution with one column key"),
		},
	} {
		memqdb, err := prepareDB(ctx)
		assert.NoError(t, err)
		mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil)
		err = innerAlterDistributionAttach(ctx, mngr, testData.dsId, testData.rel)
		if testData.err != nil {
			is.EqualError(err, testData.err.Error())
		} else {
			is.NoError(err)
			distr, err := mngr.GetDistribution(ctx, testData.dsId)
			is.NoError(err)
			_, ok := distr.Relations[testData.rel[0].Name]
			is.True(ok)
		}
	}
}
