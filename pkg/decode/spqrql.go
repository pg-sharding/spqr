package decode

import (
	"fmt"
	"strings"

	protos "github.com/pg-sharding/spqr/pkg/protos"
)

// KeyRange returns query to create given key range
// TODO unit tests
func KeyRange(krg *protos.KeyRangeInfo) string {
	/* TODO: composite key support */

	return fmt.Sprintf("CREATE KEY RANGE %s FROM %s ROUTE TO %s FOR DISTRIBUTION %s;", krg.Krid, krg.KeyRange.LowerBound, krg.ShardId, krg.DistributionId)
}

// Distribution returns query to create given distribution
// TODO unit tests
func Distribution(ds *protos.Distribution) string {
	return fmt.Sprintf("CREATE DISTRIBUTION %s COLUMN TYPES %s;", ds.Id, strings.Join(ds.ColumnTypes, ", "))
}

// DistributedRelation return query to attach relation to distribution
// TODO unit tests
func DistributedRelation(rel *protos.DistributedRelation, ds string) string {
	elems := make([]string, len(rel.DistributionKey))
	for j, el := range rel.DistributionKey {
		elems[j] = fmt.Sprintf("%s HASH FUNCTION %s", el.Column, el.HashFunction)
	}
	return fmt.Sprintf("ALTER DISTRIBUTION %s ATTACH RELATION %s DISTRIBUTION KEY %s;", ds, rel.Name, strings.Join(elems, ", "))
}
