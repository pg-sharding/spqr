package ops

import (
	"context"
	"fmt"

	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
)

var ErrRuleIntersect = fmt.Errorf("sharding rule intersects with existing one")

func AddKeyRangeWithChecks(ctx context.Context, qdb qdb.QDB, keyRange *qdb.KeyRange) error {
	spqrlog.Logger.Printf(spqrlog.DEBUG1, "adding key range %+v", keyRange)

	// TODO: do real validate
	//if err := validateShard(ctx, qdb, keyRange.ShardID); err != nil {
	//	return err
	//}

	if _, err := qdb.GetKeyRange(ctx, keyRange.KeyRangeID); err == nil {
		return fmt.Errorf("key range %v already present in qdb", keyRange.KeyRangeID)
	}

	existsKrids, err := qdb.ListKeyRanges(ctx)
	if err != nil {
		return err
	}
	spqrlog.Logger.Printf(spqrlog.DEBUG4, "keys present in qdb: %+v", existsKrids)

	for _, v := range existsKrids {
		if kr.CmpRangesLess(keyRange.LowerBound, v.LowerBound) && kr.CmpRangesLess(v.LowerBound, keyRange.UpperBound) ||
			kr.CmpRangesLess(keyRange.LowerBound, v.UpperBound) && kr.CmpRangesLess(v.UpperBound, keyRange.UpperBound) {
			return fmt.Errorf("key range %v intersects with %v present in qdb", keyRange.KeyRangeID, v.KeyRangeID)
		}
	}

	return qdb.AddKeyRange(ctx, keyRange)
}

func MatchShardingRule(ctx context.Context, qdb qdb.QDB, relationName string, shardingEntries []string) (*qdb.ShardingRule, error) {
	rules, err := qdb.ListShardingRules(ctx)
	if err != nil {
		return nil, err
	}
	spqrlog.Logger.Printf(spqrlog.DEBUG5, "checking relation %s with %d sharding rules", relationName, len(rules))

	/*
	* Create set to search column names in `shardingEntries`
	 */
	checkSet := make(map[string]struct{}, len(shardingEntries))

	for _, k := range shardingEntries {
		checkSet[k] = struct{}{}
	}

	for _, rule := range rules {
		spqrlog.Logger.Printf(spqrlog.DEBUG5, "checking %+v against %+v", rule.Entries, shardingEntries)
		// Simple optimisation
		if len(rule.Entries) > len(shardingEntries) {
			continue
		}

		allColumnsMatched := true

		for _, v := range rule.Entries {
			if _, ok := checkSet[v.Column]; !ok {
				allColumnsMatched = false
				break
			}
		}

		/* In this rule, we successfully matched all columns */
		if allColumnsMatched {
			return rule, ErrRuleIntersect
		}
	}

	return nil, nil
}

func ModifyKeyRangeWithChecks(ctx context.Context, qdb qdb.QDB, keyRange *qdb.KeyRange) error {
	// TODO: check lock are properly hold while updating

	if _, err := qdb.GetShard(ctx, keyRange.ShardID); err != nil {
		return err
	}

	krids, err := qdb.ListKeyRanges(ctx)
	if err != nil {
		return err
	}

	for _, v := range krids {
		spqrlog.Logger.Printf(spqrlog.DEBUG5, "checking with %s", v.KeyRangeID)
		if v.KeyRangeID == keyRange.KeyRangeID {
			// update req
			continue
		}
		if kr.CmpRangesLess(keyRange.LowerBound, v.LowerBound) && kr.CmpRangesLess(v.LowerBound, keyRange.UpperBound) || kr.CmpRangesLess(keyRange.LowerBound, v.UpperBound) && kr.CmpRangesLess(v.UpperBound, keyRange.UpperBound) {
			return fmt.Errorf("key range %v intersects with %v present in qdb", keyRange.KeyRangeID, v.KeyRangeID)
		}
	}

	return qdb.UpdateKeyRange(ctx, keyRange)
}
