package meta

import (
	"context"
	"fmt"
	"math"

	"github.com/pg-sharding/spqr/pkg/clientinteractor"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

type DefaultShardManager struct {
	distribution distributions.Distribution
	mngr         EntityMgr
}

func NewDefaultShardManager(distribution distributions.Distribution,
	mngr EntityMgr) *DefaultShardManager {
	return &DefaultShardManager{
		distribution: distribution,
		mngr:         mngr,
	}
}

func (manager *DefaultShardManager) DefaultKeyRangeId() string {
	return manager.distribution.Id + "." + spqrparser.DEFAULT_KEY_RANGE_SUFFIX
}

func DefaultRangeLowerBound(colTypes []string) (kr.KeyRangeBound, error) {
	lowerBound := make(kr.KeyRangeBound, len(colTypes))
	for i, colType := range colTypes {
		switch colType {
		case qdb.ColumnTypeVarchar:
			lowerBound[i] = ""
		case qdb.ColumnTypeInteger:
			lowerBound[i] = int64(math.MinInt64)
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
			ID:           manager.DefaultKeyRangeId(),
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
		return fmt.Errorf("shard '%s' for default is not exists", defaultShard.ID)
	} else {
		return manager.CreateDefaultShardNoCheck(ctx, defaultShard)
	}
}

func (manager *DefaultShardManager) CreateDefaultShardNoCheck(ctx context.Context,
	defaultShard *topology.DataShard) error {
	req, err := manager.keyRangeDefault(defaultShard.ID)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("error (1) when adding default key range for: " +
			manager.distribution.Id)
		return err
	}
	if err := manager.mngr.CreateKeyRange(ctx, req); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("error (2) when adding default key range for: " +
			manager.distribution.Id)
		return err
	}
	return nil
}

func (manager *DefaultShardManager) DropDefaultShard(ctx context.Context) (*string, error) {
	if defaultKeyRange, err := manager.mngr.GetKeyRange(ctx, manager.DefaultKeyRangeId()); err != nil {
		return nil, fmt.Errorf("distribution id=%s have not default shard", manager.distribution.Id)
	} else {
		spqrlog.Zero.Debug().Str("default key range", defaultKeyRange.ID).Msg("parsed drop")
		return &(defaultKeyRange.ShardID), manager.mngr.DropKeyRange(ctx, defaultKeyRange.ID)
	}
}

func (manager *DefaultShardManager) SuccessDropResponse(defaultShard string) clientinteractor.SimpleResultMsg {
	info := []clientinteractor.SimpleResultRow{
		clientinteractor.SimpleResultRow{Name: "distribution id", Value: manager.distribution.Id},
		clientinteractor.SimpleResultRow{Name: "shard id", Value: defaultShard},
	}
	return clientinteractor.SimpleResultMsg{Header: "drop default shard", Rows: info}
}

func (manager *DefaultShardManager) SuccessCreateResponse(defaultShard string) clientinteractor.SimpleResultMsg {
	info := []clientinteractor.SimpleResultRow{
		clientinteractor.SimpleResultRow{Name: "distribution id", Value: manager.distribution.Id},
		clientinteractor.SimpleResultRow{Name: "shard id", Value: defaultShard},
	}
	return clientinteractor.SimpleResultMsg{Header: "create default shard", Rows: info}
}
