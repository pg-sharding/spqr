package decode

import (
	"testing"

	"github.com/pg-sharding/spqr/pkg/models/kr"
	protos "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/stretchr/testify/assert"
)

// TestKeyRange is a unit test function for the KeyRange function.
//
// It tests the KeyRange function by creating a key range with the given parameters and
// asserting that the returned string matches the expected value.
//
// Parameters:
// - t (*testing.T): The testing object used for assertions.
//
// Returns:
// - None.
func TestKeyRange(t *testing.T) {
	assert := assert.New(t)
	assert.Equal("CREATE KEY RANGE kr1 FROM 10 ROUTE TO sh1 FOR DISTRIBUTION ds1;",
		KeyRange(&kr.KeyRange{
			ID:           "kr1",
			ShardID:      "sh1",
			Distribution: "ds1",
			LowerBound:   []any{10},

			ColumnTypes: []string{qdb.ColumnTypeInteger},
		}))
	// UpperBound is ignored
	assert.Equal("CREATE KEY RANGE kr1 FROM 10 ROUTE TO sh1 FOR DISTRIBUTION ds1;",
		KeyRange(
			&kr.KeyRange{
				ID:           "kr1",
				ShardID:      "sh1",
				Distribution: "ds1",
				LowerBound:   []any{10},

				ColumnTypes: []string{qdb.ColumnTypeInteger},
			}))
}

// TestDistribution is a unit test function for the Distribution function.
//
// It tests the Distribution function by creating a distribution with the given parameters and
// asserting that the returned string matches the expected value.
//
// Parameters:
// - t (*testing.T): The testing object used for assertions.
//
// Returns:
// - None.
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

// TestDistributedRelation is a unit test function for the DistributedRelation function.
//
// It tests the DistributedRelation function by asserting that the returned string matches the expected string.
//
// Parameters:
// - t (*testing.T): The testing object used for assertions.
//
// Returns:
// - None.
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
