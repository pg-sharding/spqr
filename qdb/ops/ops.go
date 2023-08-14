package ops

import (
	"context"
	"fmt"

	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/shrule"
	"github.com/pg-sharding/spqr/qdb"
)

var ErrRuleIntersect = fmt.Errorf("sharding rule intersects with existing one")

func AddShardingRuleWithChecks(ctx context.Context, qdb qdb.QDB, rule *shrule.ShardingRule) error {
	if _, err := qdb.GetShardingRule(ctx, rule.Id); err == nil {
		return fmt.Errorf("sharding rule %v already present in qdb", rule.Id)
	}

	existsRules, err := qdb.ListShardingRules(ctx)
	if err != nil {
		return err
	}

	for _, v := range existsRules {
		v_gen := shrule.ShardingRuleFromDB(v)
		if rule.Includes(v_gen) {
			return fmt.Errorf("sharding rule %v inlude existing rule %v", rule.Id, v_gen.Id)
		}
		if v_gen.Includes(rule) {
			return fmt.Errorf("sharding rule %v included in %v present in qdb", rule.Id, v_gen.Id)
		}
	}

	return qdb.AddShardingRule(ctx, shrule.ShardingRuleToDB(rule))
}

func AddKeyRangeWithChecks(ctx context.Context, qdb qdb.QDB, keyRange *kr.KeyRange) error {
	// TODO: do real validate
	//if err := validateShard(ctx, qdb, keyRange.ShardID); err != nil {
	//	return err
	//}

	if _, err := qdb.GetKeyRange(ctx, keyRange.ID); err == nil {
		return fmt.Errorf("key range %v already present in qdb", keyRange.ID)
	}

	existsKrids, err := qdb.ListKeyRanges(ctx)
	if err != nil {
		return err
	}

	for _, v := range existsKrids {
		if kr.CmpRangesLessEqual(keyRange.LowerBound, v.LowerBound) && kr.CmpRangesLess(v.LowerBound, keyRange.UpperBound) ||
			kr.CmpRangesLess(keyRange.LowerBound, v.UpperBound) && kr.CmpRangesLess(v.UpperBound, keyRange.UpperBound) ||
			kr.CmpRangesLess(v.LowerBound, keyRange.UpperBound) && kr.CmpRangesLess(keyRange.UpperBound, v.UpperBound) {
			return fmt.Errorf("key range %v intersects with %v present in qdb", keyRange.ID, v.KeyRangeID)
		}
	}

	return qdb.AddKeyRange(ctx, keyRange.ToDB())
}

func MatchShardingRule(ctx context.Context, mgr meta.EntityMgr, relationName string, shardingEntries []string, db qdb.QDB) (*qdb.ShardingRule, error) {
	/*
	* Create set to search column names in `shardingEntries`
	 */
	checkSet := make(map[string]struct{}, len(shardingEntries))

	for _, k := range shardingEntries {
		checkSet[k] = struct{}{}
	}

	var mrule *qdb.ShardingRule

	mrule = nil

	err := db.MatchShardingRules(ctx, func(rules map[string]*qdb.ShardingRule) error {
		for _, rule := range rules {
			// Simple optimisation
			if len(rule.Entries) > len(shardingEntries) {
				continue
			}

			if rule.TableName != "" && rule.TableName != relationName {
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
				mrule = rule
				return ErrRuleIntersect
			}
		}

		return nil
	})

	return mrule, err
}

func ModifyKeyRangeWithChecks(ctx context.Context, qdb qdb.QDB, keyRange *kr.KeyRange) error {
	// TODO: check lock are properly hold while updating

	if _, err := qdb.GetShard(ctx, keyRange.ShardID); err != nil {
		return err
	}

	krids, err := qdb.ListKeyRanges(ctx)
	if err != nil {
		return err
	}

	for _, v := range krids {
		if v.KeyRangeID == keyRange.ID {
			// update req
			continue
		}
		if kr.CmpRangesLessEqual(keyRange.LowerBound, v.LowerBound) && kr.CmpRangesLess(v.LowerBound, keyRange.UpperBound) ||
			kr.CmpRangesLess(keyRange.LowerBound, v.UpperBound) && kr.CmpRangesLess(v.UpperBound, keyRange.UpperBound) ||
			kr.CmpRangesLess(v.LowerBound, keyRange.UpperBound) && kr.CmpRangesLess(keyRange.UpperBound, v.UpperBound) {
			return fmt.Errorf("key range %v intersects with %v present in qdb", keyRange.ID, v.KeyRangeID)
		}
	}

	return qdb.UpdateKeyRange(ctx, keyRange.ToDB())
}
