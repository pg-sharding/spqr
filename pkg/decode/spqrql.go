package decode

import (
	"fmt"

	protos "github.com/pg-sharding/spqr/pkg/protos"
)

// TODO : unit tests
func DecodeKeyRange(krg *protos.KeyRangeInfo) string {
	/* TODO: composite key support */

	return fmt.Sprintf("CREATE KEY RANGE %s IN DATASPACE %s FROM %s ROUTE TO %s", krg.Krid, krg.KeyspaceId, krg.Bound.LowerBound[0], krg.ShardId)
}
