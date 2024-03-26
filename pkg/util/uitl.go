package util

import (
	"fmt"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"strings"
)

// GetKRCondition returns SQL condition for elements of distributed relation between two key ranges
// TODO support multidimensional key ranges
func GetKRCondition(ds *distributions.Distribution, rel *distributions.DistributedRelation, kRange *kr.KeyRange, upperBound kr.KeyRangeBound, prefix string) string {
	buf := make([]string, len(rel.DistributionKey))
	for i, entry := range rel.DistributionKey {
		// TODO remove after multidimensional key range support
		if i > 0 {
			break
		}
		// TODO add hash (depends on col type)
		hashedCol := ""
		if prefix != "" {
			hashedCol = fmt.Sprintf("%s.%s", prefix, entry.Column)
		} else {
			hashedCol = entry.Column
		}
		lBound := ""
		if ds.ColTypes[i] == "varchar" {
			lBound = fmt.Sprintf("'%s'", string(kRange.LowerBound))
		} else {
			lBound = fmt.Sprintf("%s", string(kRange.LowerBound))
		}
		if upperBound != nil {
			rBound := ""
			if ds.ColTypes[i] == "varchar" {
				rBound = fmt.Sprintf("'%s'", string(upperBound))
			} else {
				rBound = fmt.Sprintf("%s", string(upperBound))
			}
			buf[i] = fmt.Sprintf("%s >= %s AND %s < %s", hashedCol, lBound, hashedCol, rBound)
		} else {
			buf[i] = fmt.Sprintf("%s >= %s", hashedCol, lBound)
		}
	}
	return strings.Join(buf, " AND ")
}
