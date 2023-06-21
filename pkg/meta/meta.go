package meta

import (
	"context"
	"fmt"
	"strings"

	"github.com/pg-sharding/spqr/pkg/clientinteractor"
	"github.com/pg-sharding/spqr/pkg/models/dataspaces"
	"github.com/pg-sharding/spqr/pkg/models/topology"

	"github.com/pg-sharding/spqr/pkg/models/datashards"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/shrule"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

type EntityMgr interface {
	kr.KeyRangeMgr
	shrule.ShardingRulesMgr
	topology.RouterMgr
	datashards.ShardsMgr
	dataspaces.DataspaceMgr

	ShareKeyRange(id string) error
}

var unknownCoordinatorCommand = fmt.Errorf("unknown coordinator cmd")

func processDrop(ctx context.Context, dstmt spqrparser.Statement, mngr EntityMgr, cli *clientinteractor.PSQLInteractor) error {
	switch stmt := dstmt.(type) {
	case *spqrparser.KeyRangeSelector:
		if stmt.KeyRangeID == "*" {
			if err := mngr.DropKeyRangeAll(ctx); err != nil {
				return cli.ReportError(err)
			} else {
				return cli.DropKeyRange(ctx, []string{})
			}
		} else {
			spqrlog.Logger.Printf(spqrlog.DEBUG2, "parsed drop %s", stmt.KeyRangeID)
			err := mngr.DropKeyRange(ctx, stmt.KeyRangeID)
			if err != nil {
				return cli.ReportError(err)
			}
			return cli.DropKeyRange(ctx, []string{stmt.KeyRangeID})
		}
	case *spqrparser.ShardingRuleSelector:
		if stmt.ID == "*" {
			if rules, err := mngr.DropShardingRuleAll(ctx); err != nil {
				return cli.ReportError(err)
			} else {
				return cli.DropShardingRule(ctx, func() string {
					var ret []string

					for _, rule := range rules {
						ret = append(ret, rule.ID())
					}

					return strings.Join(ret, ",")
				}())
			}
		} else {
			spqrlog.Logger.Printf(spqrlog.DEBUG2, "parsed drop %s", stmt.ID)
			err := mngr.DropShardingRule(ctx, stmt.ID)
			if err != nil {
				return cli.ReportError(err)
			}
			return cli.DropShardingRule(ctx, stmt.ID)
		}
	default:
		return fmt.Errorf("unknown drop statement")
	}
}

func processCreate(ctx context.Context, astmt spqrparser.Statement, mngr EntityMgr, cli *clientinteractor.PSQLInteractor) error {
	switch stmt := astmt.(type) {
	case *spqrparser.DataspaceDefinition:
		dataspace := dataspaces.NewDataspace(stmt.ID)
		err := mngr.AddDataspace(ctx, dataspace)
		if err != nil {
			return err
		}
		return cli.AddDataspace(ctx, dataspace)
	case *spqrparser.ShardingRuleDefinition:
		entries := make([]shrule.ShardingRuleEntry, 0)
		for _, el := range stmt.Entries {
			entries = append(entries, *shrule.NewShardingRuleEntry(el.Column, el.HashFunction))
		}
		shardingRule := shrule.NewShardingRule(stmt.ID, stmt.TableName, entries)
		err := mngr.AddShardingRule(ctx, shardingRule)
		if err != nil {
			return err
		}
		return cli.AddShardingRule(ctx, shardingRule)
	case *spqrparser.KeyRangeDefinition:
		req := kr.KeyRangeFromSQL(stmt)
		if err := mngr.AddKeyRange(ctx, req); err != nil {
			return cli.ReportError(err)
		}
		return cli.AddKeyRange(ctx, req)
	default:
		return unknownCoordinatorCommand
	}
}

func Proc(ctx context.Context, tstmt spqrparser.Statement, mgr EntityMgr, cli *clientinteractor.PSQLInteractor) error {
	switch stmt := tstmt.(type) {
	case *spqrparser.Drop:
		return processDrop(ctx, stmt.Element, mgr, cli)
	case *spqrparser.Create:
		return processCreate(ctx, stmt.Element, mgr, cli)
	case *spqrparser.MoveKeyRange:
		move := &kr.MoveKeyRange{
			ShardId: stmt.DestShardID,
			Krid:    stmt.KeyRangeID,
		}

		if err := mgr.Move(ctx, move); err != nil {
			return cli.ReportError(err)
		}

		return cli.MoveKeyRange(ctx, move)

	case *spqrparser.RegisterRouter:
		newRouter := &topology.Router{
			ID:      stmt.ID,
			Address: stmt.Addr,
		}

		if err := mgr.RegisterRouter(ctx, newRouter); err != nil {
			return err
		}

		if err := mgr.SyncRouterMetadata(ctx, newRouter); err != nil {
			return err
		}

		return cli.RegisterRouter(ctx, stmt.ID, stmt.Addr)
	case *spqrparser.UnregisterRouter:
		if err := mgr.UnregisterRouter(ctx, stmt.ID); err != nil {
			return err
		}
		return cli.UnregisterRouter(stmt.ID)
	case *spqrparser.Lock:
		if _, err := mgr.LockKeyRange(ctx, stmt.KeyRangeID); err != nil {
			return err
		}
		return cli.LockKeyRange(ctx, stmt.KeyRangeID)
	case *spqrparser.Unlock:
		if err := mgr.Unlock(ctx, stmt.KeyRangeID); err != nil {
			return err
		}
		return cli.UnlockKeyRange(ctx, stmt.KeyRangeID)
	case *spqrparser.Show:
		return ProcessShow(ctx, stmt, mgr, cli)
	default:
		return unknownCoordinatorCommand
	}
}

func ProcessShow(ctx context.Context, stmt *spqrparser.Show, mngr EntityMgr, cli *clientinteractor.PSQLInteractor) error {
	spqrlog.Logger.Printf(spqrlog.DEBUG4, "show %s stmt", stmt.Cmd)
	switch stmt.Cmd {
	case spqrparser.ShowShardsStr:
		shards, err := mngr.ListShards(ctx)
		if err != nil {
			return err
		}
		var resp []*datashards.DataShard
		for _, sh := range shards {
			resp = append(resp, &datashards.DataShard{
				ID: sh.ID,
			})
		}
		return cli.Shards(ctx, resp)
	case spqrparser.ShowKeyRangesStr:
		ranges, err := mngr.ListKeyRanges(ctx)
		if err != nil {
			return err
		}
		return cli.KeyRanges(ranges)
	case spqrparser.ShowRoutersStr:
		resp, err := mngr.ListRouters(ctx)
		if err != nil {
			return err
		}

		return cli.Routers(resp)
	case spqrparser.ShowShardingRules:
		resp, err := mngr.ListShardingRules(ctx)
		if err != nil {
			return err
		}

		return cli.ShardingRules(ctx, resp)
	default:
		return unknownCoordinatorCommand
	}
}
