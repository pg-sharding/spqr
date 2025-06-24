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

func NewDefaultShardManager(distribution distributions.Distribution, mngr EntityMgr) *DefaultShardManager {
	return &DefaultShardManager{
		distribution: distribution,
		mngr:         mngr,
	}
}

func TryNewDefaultShardManager(ctx context.Context,
	distributionId string, mngr EntityMgr) (*DefaultShardManager, error) {
	if distribution, err := mngr.GetDistribution(ctx, distributionId); err != nil {
		return nil, fmt.Errorf("distribution id=%s not exists", distributionId)
	} else {
		return &DefaultShardManager{
			distribution: *distribution,
			mngr:         mngr,
		}, nil
	}
}

func (dsm *DefaultShardManager) DefaultKeyRangeId() string {
	return dsm.distribution.Id + "." + spqrparser.DEFAULT_KEY_RANGE_SUFFIX
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
			return nil, fmt.Errorf("unsuported type '%v' for default key range", colType)
		}
	}
	return lowerBound, nil
}

func (dsm *DefaultShardManager) keyRangeDefault(DefaultShardId string) (*kr.KeyRange, error) {
	if lowerBound, err := DefaultRangeLowerBound(dsm.distribution.ColTypes); err == nil {
		keyRange := &kr.KeyRange{
			ShardID:      DefaultShardId,
			ID:           dsm.DefaultKeyRangeId(),
			Distribution: dsm.distribution.Id,
			ColumnTypes:  dsm.distribution.ColTypes,
			LowerBound:   lowerBound,
		}

		return keyRange, nil
	} else {
		return nil, err
	}
}

func (dsm *DefaultShardManager) CreateDefaultShard(ctx context.Context, defaultShardId string) error {
	if defaultShard, err := dsm.mngr.GetShard(ctx, defaultShardId); err != nil {
		return fmt.Errorf("Shard '%s' for default is not exists", defaultShard.ID)
	} else {
		return dsm.CreateDefaultShardNoCheck(ctx, defaultShard)
	}
}

func (dsm *DefaultShardManager) CreateDefaultShardNoCheck(ctx context.Context, defaultShard *topology.DataShard) error {
	req, err := dsm.keyRangeDefault(defaultShard.ID)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("Error (1) when adding default key range for: " + dsm.distribution.Id)
		return err
	}
	if err := dsm.mngr.CreateKeyRange(ctx, req); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("Error (2) when adding default key range for: " + dsm.distribution.Id)
		return err
	}
	return nil
}

func (dsm *DefaultShardManager) DropDefaultShard(ctx context.Context) (*string, error) {
	if defaultKeyRange, err := dsm.mngr.GetKeyRange(ctx, dsm.DefaultKeyRangeId()); err != nil {
		return nil, fmt.Errorf("distribution id=%s have not default shard", dsm.distribution.Id)
	} else {
		spqrlog.Zero.Debug().Str("default key range", defaultKeyRange.ID).Msg("parsed drop")
		return &(defaultKeyRange.ShardID), dsm.mngr.DropKeyRange(ctx, defaultKeyRange.ID)
	}
}

func (dsm *DefaultShardManager) SuccessDropResponce(defaultShard string) clientinteractor.SimpleResultMsg {
	info := []clientinteractor.SimpleResultRow{
		clientinteractor.SimpleResultRow{Name: "distribution id", Value: dsm.distribution.Id},
		clientinteractor.SimpleResultRow{Name: "shard id", Value: defaultShard},
	}
	return clientinteractor.SimpleResultMsg{Header: "drop default shard", Rows: info}
}

func (dsm *DefaultShardManager) SuccessCreateResponce(defaultShard string) clientinteractor.SimpleResultMsg {
	info := []clientinteractor.SimpleResultRow{
		clientinteractor.SimpleResultRow{Name: "distribution id", Value: dsm.distribution.Id},
		clientinteractor.SimpleResultRow{Name: "shard id", Value: defaultShard},
	}
	return clientinteractor.SimpleResultMsg{Header: "create default shard", Rows: info}
}
