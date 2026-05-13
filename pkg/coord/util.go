package coord

import (
	"context"
	"fmt"
	"strings"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/datatransfers"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	mtran "github.com/pg-sharding/spqr/pkg/models/transaction"
	proto "github.com/pg-sharding/spqr/pkg/protos"
)

func UpdateKeyRangeMeta(ctx context.Context, gossipRequests []*proto.MetaTransactionGossipCommand) error {
	for _, gossipRequest := range gossipRequests {
		reqType, _ := mtran.GetGossipRequestType(gossipRequest)
		switch reqType {
		case mtran.GRCreateKeyRange:
			if err := updateKeyRangeMetaOnShard(ctx, gossipRequest.CreateKeyRange.KeyRangeInfo.ShardId, datatransfers.InsertKeyRangeMeta, gossipRequest.CreateKeyRange.KeyRangeInfo.Krid); err != nil {
				return err
			}
		case mtran.GRDropKeyRange:
			if err := updateKeyRangeMetaOnShard(ctx, "", datatransfers.DeleteKeyRangeMeta, gossipRequest.DropKeyRange.Id); err != nil {
				return err
			}
		}
	}
	return nil
}

func updateKeyRangeMetaOnShard(ctx context.Context, shardId string, query string, args ...any) error {
	if !config.CoordinatorConfig().ForbidDirectShardQueries {
		return nil
	}
	conns, err := config.LoadShardDataCfg(config.CoordinatorConfig().ShardDataCfg)
	if err != nil {
		return err
	}

	shardDatas := make([]*config.ShardConnect, 0)
	if shardId != "" {
		shardData, ok := conns.ShardsData[shardId]
		if !ok {
			return spqrerror.New(spqrerror.SPQR_METADATA_CORRUPTION, fmt.Sprintf("could not update key range on shard: shard \"%s\" does not exist in shard data config", shardId))
		}
		shardDatas = append(shardDatas, shardData)
	}
	errs := make([]string, 0)
	for _, shardData := range shardDatas {
		if err := func() error {
			conn, err := datatransfers.GetMasterConnection(ctx, shardData, "key_range_meta_update")
			if err != nil {
				return err
			}

			tx, err := conn.Begin(ctx)
			if err != nil {
				return err
			}

			defer func() {
				_ = tx.Rollback(ctx)
			}()

			if _, err := tx.Exec(ctx, query, args...); err != nil {
				return err
			}

			if err := tx.Commit(ctx); err != nil {
				return err
			}
			return nil
		}(); err != nil {
			errs = append(errs, err.Error())
		}
	}

	if len(errs) > 0 {
		return spqrerror.NewByCode(spqrerror.SPQR_UNEXPECTED).Detail(fmt.Sprintf("failed to update key range metadata on shard: %s", strings.Join(errs, "; ")))
	}
	return nil
}
