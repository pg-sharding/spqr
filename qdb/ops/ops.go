package ops

import (
	"context"
	"fmt"

	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
)

func validateShard(ctx context.Context, qdb qdb.QrouterDB, id string) error {
	_, err := qdb.GetShardInfo(ctx, id)
	return err
}

func AddKeyRangeWithChecks(ctx context.Context, qdb qdb.QrouterDB, keyRange *qdb.KeyRange) error {
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

	for _, v := range existsKrids {
		if kr.CmpRanges(keyRange.LowerBound, v.LowerBound) && kr.CmpRanges(v.LowerBound, keyRange.UpperBound) || kr.CmpRanges(keyRange.LowerBound, v.UpperBound) && kr.CmpRanges(v.UpperBound, keyRange.UpperBound) {
			return fmt.Errorf("key range %v intersects with %v present in qdb", keyRange.KeyRangeID, v.KeyRangeID)
		}
	}

	return qdb.AddKeyRange(ctx, keyRange)
}

func CheckShardingRule(ctx context.Context, qdb qdb.QrouterDB, colnames []string) error {
	rules, err := qdb.ListShardingRules(ctx)
	if err != nil {
		return err
	}
	spqrlog.Logger.Printf(spqrlog.DEBUG5, "checking with %d rules", len(rules))

	checkSet := make(map[string]struct{}, len(colnames))

	for _, k := range colnames {
		checkSet[k] = struct{}{}
	}

	for _, rule := range rules {
		spqrlog.Logger.Printf(spqrlog.DEBUG5, "checking %+v against %+v", rule.Colnames, colnames)
		if len(rule.Colnames) != len(colnames) {
			continue
		}

		fullMatch := true

		for _, v := range rule.Colnames {
			if _, ok := checkSet[v]; !ok {
				fullMatch = false
				break
			}
		}

		if fullMatch {
			return fmt.Errorf("sharding rule intersects with %s", rule.Id)
		}
	}

	return nil
}

func ModifyKeyRangeWithChecks(ctx context.Context, qdb qdb.QrouterDB, keyRange *qdb.KeyRange) error {
	// TODO: check lock are properly hold while updating

	if err := validateShard(ctx, qdb, keyRange.ShardID); err != nil {
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
		if kr.CmpRanges(keyRange.LowerBound, v.LowerBound) && kr.CmpRanges(v.LowerBound, keyRange.UpperBound) || kr.CmpRanges(keyRange.LowerBound, v.UpperBound) && kr.CmpRanges(v.UpperBound, keyRange.UpperBound) {
			return fmt.Errorf("key range %v intersects with %v present in qdb", keyRange.KeyRangeID, v.KeyRangeID)
		}
	}

	return qdb.UpdateKeyRange(ctx, keyRange)
}
