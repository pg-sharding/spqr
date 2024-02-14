package ops

import (
	"context"

	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/shrule"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/qdb"
)

// TODO : unit tests
func AddShardingRuleWithChecks(ctx context.Context, qdb qdb.QDB, rule *shrule.ShardingRule) error {
	if _, err := qdb.GetShardingRule(ctx, rule.Id); err == nil {
		return spqrerror.Newf(spqrerror.SPQR_SHARDING_RULE_ERROR, "sharding rule %v already present in qdb", rule.Id)
	}

	existDistribution, err := qdb.ListDistributions(ctx)
	if err != nil {
		return err
	}
	exists := false
	for _, ds := range existDistribution {
		exists = ds.ID == rule.Distribution
		if exists {
			break
		}
	}
	if !exists {
		return spqrerror.New(spqrerror.SPQR_NO_DISTRIBUTION, "try to add sharding rule link to a non-existent distribution")
	}

	existsRules, err := qdb.ListShardingRules(ctx, rule.Distribution)
	if err != nil {
		return err
	}

	for _, v := range existsRules {
		vGen := shrule.ShardingRuleFromDB(v)
		if rule.Includes(vGen) {
			return spqrerror.Newf(spqrerror.SPQR_SHARDING_RULE_ERROR, "sharding rule %v include existing rule %v", rule.Id, vGen.Id)
		}
		if vGen.Includes(rule) {
			return spqrerror.Newf(spqrerror.SPQR_SHARDING_RULE_ERROR, "sharding rule %v included in %v present in qdb", rule.Id, vGen.Id)
		}
	}

	return qdb.AddShardingRule(ctx, shrule.ShardingRuleToDB(rule))
}

// TODO : unit tests
func AddKeyRangeWithChecks(ctx context.Context, qdb qdb.QDB, keyRange *kr.KeyRange) error {
	if _, err := qdb.GetShard(ctx, keyRange.ShardID); err != nil {
		return err
	}

	if _, err := qdb.GetKeyRange(ctx, keyRange.ID); err == nil {
		return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range %v already present in qdb", keyRange.ID)
	}

	_, err := qdb.GetDistribution(ctx, keyRange.Distribution)
	if err != nil {
		return spqrerror.New(spqrerror.SPQR_NO_DISTRIBUTION, "try to add key range link to a non-existent distribution")
	}

	existsKrids, err := qdb.ListKeyRanges(ctx, keyRange.Distribution)
	if err != nil {
		return err
	}

	for _, v := range existsKrids {
		if kr.CmpRangesEqual(keyRange.LowerBound, v.LowerBound) {
			return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range %v intersects with key range %v in QDB", keyRange.ID, v.KeyRangeID)
		}
	}

	return qdb.AddKeyRange(ctx, keyRange.ToDB())
}

// TODO : unit tests
func ModifyKeyRangeWithChecks(ctx context.Context, qdb qdb.QDB, keyRange *kr.KeyRange) error {
	_, err := qdb.CheckLockedKeyRange(ctx, keyRange.ID)
	if err != nil {
		return err
	}

	if _, err := qdb.GetShard(ctx, keyRange.ShardID); err != nil {
		return err
	}

	krids, err := qdb.ListKeyRanges(ctx, keyRange.Distribution)
	if err != nil {
		return err
	}

	for _, v := range krids {
		if v.KeyRangeID == keyRange.ID {
			// update req
			continue
		}
		if kr.CmpRangesEqual(keyRange.LowerBound, v.LowerBound) {
			return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range %v intersects with key range %v in QDB", keyRange.ID, v.KeyRangeID)
		}
	}

	return qdb.UpdateKeyRange(ctx, keyRange.ToDB())
}
