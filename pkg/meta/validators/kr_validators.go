package meta_validators

import (
	"context"

	meta "github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
)

// ValidateKeyRangeForCreate validates key range before create
//
// Parameters:
// - ctx: the context of the operation.
// - mngr (meta.EntityMgr): this entity manager gets data about meta for validating key range
// - keyRange (*kr.KeyRange): key range for validating
//
// Returns:
// - error: an error if validation is not passed
func ValidateKeyRangeForCreate(ctx context.Context, mngr meta.EntityMgr, keyRange *kr.KeyRange) error {
	if _, err := mngr.GetShard(ctx, keyRange.ShardID); err != nil {
		return err
	}

	if _, err := mngr.GetKeyRange(ctx, keyRange.ID); err == nil {
		return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range %v already present in qdb", keyRange.ID)
	}

	_, err := mngr.GetDistribution(ctx, keyRange.Distribution)
	if err != nil {
		return spqrerror.New(spqrerror.SPQR_OBJECT_NOT_EXIST, "try to add key range link to a nonexistent distribution")
	}

	existsKrids, err := mngr.ListKeyRanges(ctx, keyRange.Distribution)
	if err != nil {
		return err
	}

	var nearestKr *kr.KeyRange = nil
	for _, v := range existsKrids {
		// TODO: need remove lowlevel checks with qdbKr from QDB layer
		qdbKeyRange := v.ToDB()
		eph, err := kr.KeyRangeFromBytes(qdbKeyRange.LowerBound, keyRange.ColumnTypes)
		if err != nil {
			return err
		}
		if kr.CmpRangesLessEqual(eph.LowerBound, keyRange.LowerBound, keyRange.ColumnTypes) {
			if nearestKr == nil || kr.CmpRangesLess(nearestKr.LowerBound, eph.LowerBound, nearestKr.ColumnTypes) {
				nearestKr = eph
				nearestKr.ID = qdbKeyRange.KeyRangeID
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

	return nil
}

// ValidateKeyRangeForCreate validates key range before modifying
//
// Parameters:
// - ctx: the context of the operation.
// - mngr (meta.EntityMgr): this entity manager gets data about meta for validating key range
// - keyRange (*kr.KeyRange): key range for validating
//
// Returns:
// - error: an error if validation is not passed
func ValidateKeyRangeForModify(ctx context.Context, mngr meta.EntityMgr, keyRange *kr.KeyRange) error {
	krLock, err := mngr.GetKeyRange(ctx, keyRange.ID)
	if err != nil {
		return err
	}

	if krLock.IsLocked == nil || !(*krLock.IsLocked) {
		return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range %v not locked", keyRange.ID)
	}

	if _, err := mngr.GetShard(ctx, keyRange.ShardID); err != nil {
		return err
	}

	krids, err := mngr.ListKeyRanges(ctx, keyRange.Distribution)
	if err != nil {
		return err
	}

	for _, v := range krids {
		// TODO: need remove lowlevel checks with qdbKr from QDB layer
		qdbKeyRange := v.ToDB()
		if qdbKeyRange.KeyRangeID == keyRange.ID {
			// update req
			continue
		}

		eph, err := kr.KeyRangeFromBytes(qdbKeyRange.LowerBound, keyRange.ColumnTypes)
		if err != nil {
			return err
		}
		if kr.CmpRangesEqual(keyRange.LowerBound, eph.LowerBound, keyRange.ColumnTypes) {
			return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range %v intersects with key range %v in QDB", keyRange.ID, qdbKeyRange.KeyRangeID)
		}
	}

	return nil
}
