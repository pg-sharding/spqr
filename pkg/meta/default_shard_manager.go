package meta

import (
	"context"
	"fmt"
	"math"

	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

type DefaultShardManager struct {
	distribution *distributions.Distribution
	// TODO: after converting DropKeyRange to 2 phase execution need convert to TranEntityManager
	mngr EntityMgr
}

func NewDefaultShardManager(distribution *distributions.Distribution,
	mngr EntityMgr) *DefaultShardManager {
	return &DefaultShardManager{
		distribution: distribution,
		mngr:         mngr,
	}
}

func DefaultKeyRangeId(distrib *distributions.Distribution) string {
	return distrib.Id + "." + spqrparser.DEFAULT_KEY_RANGE_SUFFIX
}

func DefaultRangeLowerBound(colTypes []string) (kr.KeyRangeBound, error) {
	lowerBound := make(kr.KeyRangeBound, len(colTypes))
	for i, colType := range colTypes {
		switch colType {
		case qdb.ColumnTypeVarchar, qdb.ColumnTypeVarcharDeprecated:
			lowerBound[i] = ""
		case qdb.ColumnTypeInteger:
			lowerBound[i] = int64(math.MinInt64)
		case qdb.ColumnTypeUinteger, qdb.ColumnTypeVarcharHashed:
			lowerBound[i] = uint64(0)
		default:
			return nil, fmt.Errorf("unsupported type '%v' for default key range", colType)
		}
	}
	return lowerBound, nil
}

func (manager *DefaultShardManager) keyRangeDefault(DefaultShardId string) (*kr.KeyRange, error) {
	if lowerBound, err := DefaultRangeLowerBound(manager.distribution.ColTypes); err == nil {
		keyRange := &kr.KeyRange{
			ShardID:      DefaultShardId,
			ID:           DefaultKeyRangeId(manager.distribution),
			Distribution: manager.distribution.Id,
			ColumnTypes:  manager.distribution.ColTypes,
			LowerBound:   lowerBound,
		}

		return keyRange, nil
	} else {
		return nil, err
	}
}

func (manager *DefaultShardManager) CreateDefaultShard(ctx context.Context, defaultShardId string) error {
	if defaultShard, err := manager.mngr.GetShard(ctx, defaultShardId); err != nil {
		return fmt.Errorf("shard '%s' does not exist", defaultShardId)
	} else {
		return manager.CreateDefaultShardNoCheck(ctx, defaultShard)
	}
}

func (manager *DefaultShardManager) CreateDefaultShardNoCheck(ctx context.Context,
	defaultShard *topology.DataShard) error {
	keyRange, err := manager.keyRangeDefault(defaultShard.ID)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("error (1) when adding default key range for: " +
			manager.distribution.Id)
		return err
	}
	tranMngr := NewTranEntityManager(manager.mngr)
	if err := CreateKeyRangeStrict(ctx, tranMngr, keyRange); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("error (2) when adding default key range for: " +
			manager.distribution.Id)
		return err
	}
	if err = tranMngr.ExecNoTran(ctx); err != nil {
		return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "failed to commit a new key range as default: %s", err.Error())
	}
	return nil
}

func (manager *DefaultShardManager) DropDefaultShard(ctx context.Context) (string, error) {
	if defaultKeyRange, err := manager.mngr.GetKeyRange(ctx, DefaultKeyRangeId(manager.distribution)); err != nil {
		return "", fmt.Errorf("distribution id=%s have not default shard", manager.distribution.Id)
	} else {
		spqrlog.Zero.Debug().Str("default key range", defaultKeyRange.ID).Msg("parsed drop")
		return defaultKeyRange.ShardID, manager.mngr.DropKeyRange(ctx, defaultKeyRange.ID)
	}
}
