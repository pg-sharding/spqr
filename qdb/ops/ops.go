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

	existDataspace, err := qdb.ListDataspaces(ctx)
	if err != nil {
		return err
	}
	hasDs := false
	for _, ds := range existDataspace {
		hasDs = ds.ID == rule.Dataspace
		if hasDs {
			break
		}
	}
	if !hasDs {
		return fmt.Errorf("try to add sharding rule link to a non-existent dataspace")
	}

	existsRules, err := qdb.ListShardingRules(ctx, rule.Dataspace)
	if err != nil {
		return err
	}

	for _, v := range existsRules {
		vGen := shrule.ShardingRuleFromDB(v)
		if rule.Includes(vGen) {
			return fmt.Errorf("sharding rule %v inlude existing rule %v", rule.Id, vGen.Id)
		}
		if vGen.Includes(rule) {
			return fmt.Errorf("sharding rule %v included in %v present in qdb", rule.Id, vGen.Id)
		}
	}

	return qdb.AddShardingRule(ctx, shrule.ShardingRuleToDB(rule))
}

func AddKeyRangeWithChecks(ctx context.Context, qdb qdb.QDB, keyRange *kr.KeyRange) error {
	if _, err := qdb.GetShard(ctx, keyRange.ShardID); err != nil {
		return err
	}

	if _, err := qdb.GetKeyRange(ctx, keyRange.ID); err == nil {
		return fmt.Errorf("key range %v already present in qdb", keyRange.ID)
	}

	existDataspace, err := qdb.ListDataspaces(ctx)
	if err != nil {
		return err
	}
	hasDs := false
	for _, ds := range existDataspace {
		hasDs = ds.ID == keyRange.Dataspace
		if hasDs {
			break
		}
	}
	if !hasDs {
		return fmt.Errorf("try to add key range link to a non-existent dataspace")
	}

	existsKrids, err := qdb.ListKeyRanges(ctx, keyRange.Dataspace)
	if err != nil {
		return err
	}

	for _, v := range existsKrids {
		if doIntersect(keyRange, v) {
			return fmt.Errorf("key range %v intersects with key range %v in QDB", keyRange.ID, v.KeyRangeID)
		}
	}

	return qdb.AddKeyRange(ctx, keyRange.ToDB())
}

func MatchShardingRule(ctx context.Context, _ meta.EntityMgr, relationName string, shardingEntries []string, db qdb.QDB) (*qdb.ShardingRule, error) {
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
	_, err := qdb.CheckLockedKeyRange(ctx, keyRange.ID)
	if err != nil {
		return err
	}

	if _, err := qdb.GetShard(ctx, keyRange.ShardID); err != nil {
		return err
	}

	krids, err := qdb.ListKeyRanges(ctx, keyRange.Dataspace)
	if err != nil {
		return err
	}

	for _, v := range krids {
		if v.KeyRangeID == keyRange.ID {
			// update req
			continue
		}
		if doIntersect(keyRange, v) {
			return fmt.Errorf("key range %v intersects with key range %v in QDB", keyRange.ID, v.KeyRangeID)
		}
	}

	return qdb.UpdateKeyRange(ctx, keyRange.ToDB())
}

// This method checks if two key ranges intersect
func doIntersect(l *kr.KeyRange, r *qdb.KeyRange) bool {
	// l0     r0      l1      r1
	// |------|-------|--------
	//
	// r0     l0      r1      l1
	// -------|-------|-------|
	//
	// l0     r0      l1      r1
	// -------|-------|-------|
	return kr.CmpRangesLessEqual(l.LowerBound, r.LowerBound) && kr.CmpRangesLess(r.LowerBound, l.UpperBound) ||
		kr.CmpRangesLess(l.LowerBound, r.UpperBound) && kr.CmpRangesLessEqual(r.UpperBound, l.UpperBound) ||
		kr.CmpRangesLess(r.LowerBound, l.UpperBound) && kr.CmpRangesLessEqual(l.UpperBound, r.UpperBound)
}
