package decode

import (
	"fmt"
	"strings"

	protos "github.com/pg-sharding/spqr/pkg/protos"
)

// KeyRange returns query to create given key range
// KeyRange returns a string representing a SQL query to create a key range.
//
// Parameters:
// - krg (*protos.KeyRangeInfo): a pointer to a KeyRangeInfo struct containing the information about the key range.
//
// Returns:
// - string: the SQL query to create the key range.
func KeyRange(krg *protos.KeyRangeInfo) string {
	/* TODO: composite key support */
	return fmt.Sprintf("CREATE KEY RANGE %s FROM %s ROUTE TO %s FOR DISTRIBUTION %s;", krg.Krid, krg.KeyRange.LowerBound, krg.ShardId, krg.DistributionId)
}

// Distribution returns query to create given distribution
// Distribution creates a SQL query to create a distribution based on the provided Distribution struct.
//
// Parameters:
// - ds (*protos.Distribution): a pointer to a Distribution struct containing the distribution information.
// Return type:
// - string: the SQL query for creating the distribution.
func Distribution(ds *protos.Distribution) string {
	return fmt.Sprintf("CREATE DISTRIBUTION %s COLUMN TYPES %s;", ds.Id, strings.Join(ds.ColumnTypes, ", "))
}

// DistributedRelation return query to attach relation to distribution
// DistributedRelation returns a SQL query to attach a distributed relation to a distribution.
//
// Parameters:
// - rel (*protos.DistributedRelation): a pointer to a DistributedRelation struct containing the information about the distributed relation.
// - ds (string): the name of the distribution to attach the relation to.
//
// Returns:
// - string: the SQL query to attach the relation to the distribution.
func DistributedRelation(rel *protos.DistributedRelation, ds string) string {
	elems := make([]string, len(rel.DistributionKey))
	for j, el := range rel.DistributionKey {
		if el.HashFunction != "" {
			elems[j] = fmt.Sprintf("%s HASH FUNCTION %s", el.Column, el.HashFunction)
		} else {
			elems[j] = el.Column
		}

	}
	return fmt.Sprintf("ALTER DISTRIBUTION %s ATTACH RELATION %s DISTRIBUTION KEY %s;", ds, rel.Name, strings.Join(elems, ", "))
}
