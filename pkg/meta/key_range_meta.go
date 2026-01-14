package meta

import (
	"context"
	"fmt"

	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	mtran "github.com/pg-sharding/spqr/pkg/models/transaction"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
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
func ValidateKeyRangeForCreate(ctx context.Context, mngr EntityMgr, keyRange *kr.KeyRange) error {
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

// ValidateKeyRangeForModify validates key range before modifying
//
// Parameters:
// - ctx: the context of the operation.
// - mngr (meta.EntityMgr): this entity manager gets data about meta for validating key range
// - keyRange (*kr.KeyRange): key range for validating
//
// Returns:
// - error: an error if validation is not passed
func ValidateKeyRangeForModify(ctx context.Context, mngr EntityMgr, keyRange *kr.KeyRange) error {
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

func CreateKeyRangeStrict(ctx context.Context, tranMngr EntityMgr, keyRange *kr.KeyRange) (*mtran.MetaTransactionChunk, error) {
	if err := ValidateKeyRangeForCreate(ctx, tranMngr, keyRange); err != nil {
		return nil, err
	}

	chunk, err := tranMngr.CreateKeyRange(ctx, keyRange)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("Error when adding key range")
		return nil, err
	}
	return chunk, err
}

// TODO : unit tests

// createKeyRange creates key range
//
// Parameters:
// - ctx (context.Context): The context of the operation.
// - mngr (TranEntityManager): The entity manager used to manage the entities.
// - stmt (spqrparser.KeyRangeDefinition): The create distribution statement to be processed.
// Returns:
// - *kr.KeyRange: created key range.
// - error: An error if the creation encounters any issues.
func createKeyRange(ctx context.Context, tranMngr TranEntityManager, stmt *spqrparser.KeyRangeDefinition) (*kr.KeyRange, error) {
	tran, err := tranMngr.BeginTran(ctx)
	if err != nil {
		return nil, err
	}
	if stmt.Distribution.ID == "default" {
		list, err := tranMngr.ListDistributions(ctx)
		if err != nil {
			return nil, spqrerror.New(spqrerror.SPQR_OBJECT_NOT_EXIST, "error while selecting list of distributions")
		}
		if len(list) == 0 {
			return nil, spqrerror.New(spqrerror.SPQR_OBJECT_NOT_EXIST, "you don't have any distributions")
		}
		if len(list) > 1 {
			return nil, spqrerror.New(spqrerror.SPQR_OBJECT_NOT_EXIST, "distributions count not equal one, use FOR DISTRIBUTION syntax")
		}
		stmt.Distribution.ID = list[0].Id
	}
	ds, err := tranMngr.GetDistribution(ctx, stmt.Distribution.ID)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("Error when adding key range")
		return nil, err
	}
	if defaultKr := DefaultKeyRangeId(ds); stmt.KeyRangeID == defaultKr {
		err := fmt.Errorf("key range %s is reserved", defaultKr)
		spqrlog.Zero.Error().Err(err).Msg("failed to create key range")
		return nil, err
	}
	keyRange, err := kr.KeyRangeFromSQL(stmt, ds.ColTypes)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("Error when adding key range")
		return nil, err
	}
	chunk, err := CreateKeyRangeStrict(ctx, &tranMngr, keyRange)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("Error when adding key range")
		return nil, err
	}
	tranChunk, err := mtran.NewMetaTransactionChunk(chunk.GossipRequests, chunk.QdbStatements)
	if err != nil {
		return nil, err
	}
	tran.Operations = tranChunk
	if err = tranMngr.CommitTran(ctx, tran); err != nil {
		return nil, err
	}
	return keyRange, nil
}
