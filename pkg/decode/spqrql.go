package decode

import (
	"fmt"

	protos "github.com/pg-sharding/spqr/pkg/protos"
)

// TODO : unit tests
func DecodeRule(rule *protos.ShardingRule) string {
	/* TODO: composite key support */
	if rule.TableName != "" {
		return fmt.Sprintf("CREATE SHARDING RULE %s TABLE %s COLUMN %s", rule.Id, rule.TableName, rule.ShardingRuleEntry[0].Column)
	}
	return fmt.Sprintf("CREATE SHARDING RULE %s COLUMN %s", rule.Id, rule.ShardingRuleEntry[0].Column)
}

// TODO : unit tests
func DecodeKeyRange(krg *protos.KeyRangeInfo) string {
	/* TODO: composite key support */

	return fmt.Sprintf("CREATE KEY RANGE %s FROM %s TO %s ROUTE TO %s", krg.Krid, krg.KeyRange.LowerBound, krg.KeyRange.UpperBound, krg.ShardId)
}
