package decode

import (
	protos "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestKeyRange(t *testing.T) {
	assert := assert.New(t)
	assert.Equal("CREATE KEY RANGE kr1 FROM 10 ROUTE TO sh1 FOR DISTRIBUTION ds1;",
		KeyRange(&protos.KeyRangeInfo{
			Krid:           "kr1",
			ShardId:        "sh1",
			DistributionId: "ds1",
			KeyRange: &protos.KeyRange{
				LowerBound: "10",
			},
		}))
	// UpperBound is ignored
	assert.Equal("CREATE KEY RANGE kr1 FROM 10 ROUTE TO sh1 FOR DISTRIBUTION ds1;",
		KeyRange(&protos.KeyRangeInfo{
			Krid:           "kr1",
			ShardId:        "sh1",
			DistributionId: "ds1",
			KeyRange: &protos.KeyRange{
				LowerBound: "10",
				UpperBound: "20",
			},
		}))
}

func TestDistribution(t *testing.T) {
	assert := assert.New(t)
	assert.Equal("CREATE DISTRIBUTION ds1 COLUMN TYPES integer;",
		Distribution(&protos.Distribution{
			Id:          "ds1",
			ColumnTypes: []string{"integer"},
		}))

	// relations ignored
	assert.Equal("CREATE DISTRIBUTION ds1 COLUMN TYPES integer;",
		Distribution(&protos.Distribution{
			Id:          "ds1",
			ColumnTypes: []string{"integer"},
			Relations: []*protos.DistributedRelation{
				{Name: "rel", DistributionKey: []*protos.DistributionKeyEntry{{Column: "id", HashFunction: "identity"}}},
			},
		}))

	// multiple types
	assert.Equal("CREATE DISTRIBUTION ds1 COLUMN TYPES integer, varchar;",
		Distribution(&protos.Distribution{
			Id:          "ds1",
			ColumnTypes: []string{"integer", "varchar"},
		}))

	// order is preserved
	assert.Equal("CREATE DISTRIBUTION ds1 COLUMN TYPES varchar, integer;",
		Distribution(&protos.Distribution{
			Id:          "ds1",
			ColumnTypes: []string{"varchar", "integer"},
		}))
}

func TestDistributedRelation(t *testing.T) {
	assert := assert.New(t)
	assert.Equal("ALTER DISTRIBUTION ds1 ATTACH RELATION rel DISTRIBUTION KEY id HASH FUNCTION identity;",
		DistributedRelation(&protos.DistributedRelation{
			Name:            "rel",
			DistributionKey: []*protos.DistributionKeyEntry{{Column: "id", HashFunction: "identity"}},
		}, "ds1"),
	)

	// missing hash func
	assert.Equal("ALTER DISTRIBUTION ds1 ATTACH RELATION rel DISTRIBUTION KEY id;",
		DistributedRelation(&protos.DistributedRelation{
			Name:            "rel",
			DistributionKey: []*protos.DistributionKeyEntry{{Column: "id"}},
		}, "ds1"),
	)

	// multiple columns
	assert.Equal("ALTER DISTRIBUTION ds1 ATTACH RELATION rel DISTRIBUTION KEY id HASH FUNCTION identity, "+
		"id2 HASH FUNCTION city;",
		DistributedRelation(&protos.DistributedRelation{
			Name: "rel",
			DistributionKey: []*protos.DistributionKeyEntry{
				{Column: "id", HashFunction: "identity"},
				{Column: "id2", HashFunction: "city"},
			},
		}, "ds1"),
	)
}
