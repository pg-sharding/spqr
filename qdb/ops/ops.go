package ops

import (
	"context"

	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/qdb"
)

// TODO : unit tests
func CreateKeyRangeWithChecks(ctx context.Context, qdb qdb.XQDB, keyRange *kr.KeyRange) error {
	if _, err := qdb.GetShard(ctx, keyRange.ShardID); err != nil {
		return err
	}

	if _, err := qdb.GetKeyRange(ctx, keyRange.ID); err == nil {
		return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range %v already present in qdb", keyRange.ID)
	}

	_, err := qdb.GetDistribution(ctx, keyRange.Distribution)
	if err != nil {
		return spqrerror.New(spqrerror.SPQR_OBJECT_NOT_EXIST, "try to add key range link to a nonexistent distribution")
	}

	existsKrids, err := qdb.ListKeyRanges(ctx, keyRange.Distribution)
	if err != nil {
		return err
	}

	var nearestKr *kr.KeyRange = nil
	for _, v := range existsKrids {
		eph, err := kr.KeyRangeFromBytes(v.LowerBound, keyRange.ColumnTypes)
		if err != nil {
			return err
		}
		if kr.CmpRangesLessEqual(eph.LowerBound, keyRange.LowerBound, keyRange.ColumnTypes) {
			if nearestKr == nil || kr.CmpRangesLess(nearestKr.LowerBound, eph.LowerBound, nearestKr.ColumnTypes) {
				nearestKr = eph
				nearestKr.ID = v.KeyRangeID
				nearestKr.ShardID = v.ShardID
			}
		}
	}
	if nearestKr != nil && kr.CmpRangesEqual(nearestKr.LowerBound, keyRange.LowerBound, keyRange.ColumnTypes) {
		return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range %v equals key range %v in QDB", keyRange.ID, nearestKr.ID)
	}
	if nearestKr != nil && nearestKr.ShardID != keyRange.ShardID {
		return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range %v intersects with key range %v in QDB", keyRange.ID, nearestKr.ID)
	}

	return qdb.CreateKeyRange(ctx, keyRange.ToDB())
}

// TODO : unit tests
func ModifyKeyRangeWithChecks(ctx context.Context, qdb qdb.XQDB, keyRange *kr.KeyRange) error {
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

		eph, err := kr.KeyRangeFromBytes(v.LowerBound, keyRange.ColumnTypes)
		if err != nil {
			return err
		}
		if kr.CmpRangesEqual(keyRange.LowerBound, eph.LowerBound, keyRange.ColumnTypes) {
			return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range %v intersects with key range %v in QDB", keyRange.ID, v.KeyRangeID)
		}
	}

	return qdb.UpdateKeyRange(ctx, keyRange.ToDB())
}
