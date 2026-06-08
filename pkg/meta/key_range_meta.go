package meta

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/pg-sharding/spqr/coordinator/statistics"
	"github.com/pg-sharding/spqr/pkg/models/hashfunction"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
	"github.com/sethvargo/go-retry"
)

const (
	MaxLockRetry  = 7
	LockRetryStep = 500 * time.Millisecond
)

func ValidateKeyRangeForCreate(ctx context.Context, mngr EntityMgrReader, keyRange *kr.KeyRange) error {
	if _, err := mngr.GetShard(ctx, keyRange.ShardID); err != nil {
		return err
	}

	if _, err := mngr.GetKeyRange(ctx, keyRange.ID); err == nil {
		return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range %v already present in qdb", keyRange.ID)
	}

	_, err := mngr.GetDistribution(ctx, keyRange.Distribution)
	if err != nil {
		return spqrerror.New(spqrerror.SPQR_OBJECT_NOT_EXIST, "trying to add key range to a nonexistent distribution")
	}

	existsKrids, err := mngr.ListKeyRanges(ctx, keyRange.Distribution)
	if err != nil {
		return err
	}

	var nearestKr *kr.KeyRange
	for _, v := range existsKrids {
		// TODO: need remove lowlevel checks with qdbKr from QDB layer
		if kr.CmpRangesLessEqual(v.LowerBound, keyRange.LowerBound, keyRange.ColumnTypes) {
			if nearestKr == nil || kr.CmpRangesLess(nearestKr.LowerBound, v.LowerBound, nearestKr.ColumnTypes) {
				nearestKr = v
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
func ValidateKeyRangeForModify(ctx context.Context, mngr EntityMgrReader, keyRange *kr.KeyRange) error {
	krLock, err := mngr.GetKeyRange(ctx, keyRange.ID)
	if err != nil {
		return err
	}

	if !krLock.IsLocked {
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

func CreateKeyRangeStrict(ctx context.Context, mngr *TranEntityManager, keyRange *kr.KeyRange, colTypes []string) error {
	if err := ValidateKeyRangeForCreate(ctx, mngr, keyRange); err != nil {
		return err
	}

	err := mngr.CreateKeyRange(ctx, keyRange, colTypes)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("CreateKeyRange failed while CreateKeyRangeStrict")
		return err
	}
	return nil
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
func createKeyRange(ctx context.Context, mngr *TranEntityManager, stmt *spqrparser.KeyRangeDefinition, beginTran bool) (*kr.KeyRange, error) {
	if beginTran {
		if err := mngr.BeginTran(ctx); err != nil {
			return nil, err
		}
	}
	if stmt.Distribution.ID == "default" {
		list, err := mngr.ListDistributions(ctx)
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
	ds, err := mngr.GetDistribution(ctx, stmt.Distribution.ID)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("GetDistribution failed while createKeyRange")
		return nil, err
	}
	if defaultKr := DefaultKeyRangeId(ds); stmt.KeyRangeID == defaultKr {
		err := fmt.Errorf("key range %s is reserved", defaultKr)
		spqrlog.Zero.Error().
			Str("key_range", defaultKr).
			Msg("the key range is reserved, failed to create key range")
		return nil, err
	}
	keyRange, err := kr.KeyRangeFromSQL(stmt, ds.ColTypes)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("KeyRangeFromSQL failed while createKeyRange")
		return nil, err
	}
	err = CreateKeyRangeStrict(ctx, mngr, keyRange, ds.ColTypes)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("CreateKeyRangeStrict failed while createKeyRange")
		return nil, err
	}
	if beginTran {
		if err = mngr.CommitTran(ctx); err != nil {
			return nil, err
		}
	}
	return keyRange, nil
}

func createKeyRangesForDistribution(ctx context.Context, mngr *TranEntityManager, stmt *spqrparser.KeyRangesForDistributionDefinition) ([]*kr.KeyRange, error) {
	selectedShards := make([]*topology.DataShard, 0)
	if len(stmt.Shards) == 1 && stmt.Shards[0] == "*" {
		shards, err := mngr.ListShards(ctx)
		if err != nil {
			return nil, err
		}
		selectedShards = shards
	} else {
		for _, sh := range stmt.Shards {
			shard, err := mngr.GetShard(ctx, sh)
			if err != nil {
				return nil, err
			}
			selectedShards = append(selectedShards, shard)
		}
	}

	distr, err := mngr.GetDistribution(ctx, stmt.Distribution.ID)
	if err != nil {
		return nil, err
	}

	bounds, err := splitEqualFullKeyRange(distr.ColTypes, len(selectedShards))
	if err != nil {
		return nil, err
	}

	createdKrs := make([]*kr.KeyRange, 0, len(selectedShards))
	if err := mngr.BeginTran(ctx); err != nil {
		return nil, err
	}
	for i, bound := range bounds {
		spqrlog.Zero.Debug().Interface("bound", bound).Msg("lower bound here222")
		newKr, err := createKeyRange(ctx, mngr, &spqrparser.KeyRangeDefinition{
			Distribution: stmt.Distribution,
			ShardID:      selectedShards[i].ID,
			LowerBound:   &spqrparser.KeyRangeBound{Pivots: bound},
			KeyRangeID:   fmt.Sprintf("%s-%d", stmt.Distribution.ID, i),
		}, false)
		if err != nil {
			return nil, err
		}
		createdKrs = append(createdKrs, newKr)
	}
	if err := mngr.CommitTran(ctx); err != nil {
		return nil, err
	}

	return createdKrs, nil
}

func splitEqualFullKeyRange(colTypes []string, shardsNumber int) ([][][]byte, error) {
	bounds := make([][][]byte, shardsNumber)
	for shInd := range shardsNumber {
		bounds[shInd] = make([][]byte, len(colTypes))

		for i, t := range colTypes {
			switch t {
			case qdb.ColumnTypeVarcharDeprecated:
				fallthrough
			case qdb.ColumnTypeUUID:
				fallthrough
			case qdb.ColumnTypeVarchar:
				//bounds[shInd][i] = []byte(values[i])
				return nil, fmt.Errorf("varchar is not supported yet, create key ranges manually")
			case qdb.ColumnTypeVarcharHashed:
				fallthrough
			case qdb.ColumnTypeUUIDHashed:
				fallthrough
			case qdb.ColumnTypeUinteger:
				delta := math.MaxUint64 / uint64(shardsNumber)
				var lowerBound uint64 = delta * uint64(shardsNumber-1-shInd)
				bounds[shInd][i] = hashfunction.EncodeUInt64(lowerBound)
			case qdb.ColumnTypeInteger:
				// FIX, consider negative bound
				delta := math.MaxInt64/int64(shardsNumber) - math.MinInt64/int64(shardsNumber)
				var lowerBound int64 = math.MinInt64 + delta*int64(shardsNumber-1-shInd)
				spqrlog.Zero.Debug().Int64("bound", lowerBound).Msg("lower bound here111")
				bounds[shInd][i] = make([]byte, binary.MaxVarintLen64)
				binary.PutVarint(bounds[shInd][i], lowerBound)
			default:
				return nil, fmt.Errorf("unknown column type: %s", t)
			}
		}
	}
	return bounds, nil
}

func dropKeyRange(ctx context.Context, mngr *TranEntityManager, id string) error {
	if err := mngr.BeginTran(ctx); err != nil {
		return err
	}
	if err := mngr.DropKeyRange(ctx, id); err != nil {
		return err
	}

	return mngr.CommitTran(ctx)
}

// locks key range with retries
//
// Parameters:
// - ctx (context.Context): The context of the operation.
// - mngr (TranEntityManager): The entity manager used to manage the entities.
// - keyRangeID (string): key range to lock
// Returns:
// - *kr.KeyRange: locked key range.
// - error: An error if the locking any issues.
func LockKeyRange(ctx context.Context, mngr EntityMgr, keyRangeID string) (*kr.KeyRange, error) {
	t := time.Now()
	if kr, err := retry.DoValue(ctx, retry.WithMaxRetries(MaxLockRetry,
		retry.NewFibonacci(LockRetryStep)),
		func(ctx context.Context) (*kr.KeyRange, error) {
			return mngr.LockKeyRange(ctx, keyRangeID)
		}); err != nil {
		statistics.RecordQDBOperation("LockKeyRange", time.Since(t))
		return nil, err
	} else {
		statistics.RecordQDBOperation("LockKeyRange", time.Since(t))
		return kr, nil
	}

}
