package meta

import (
	"context"
	"fmt"
	"strings"

	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/clientinteractor"
	"github.com/pg-sharding/spqr/pkg/models/datashards"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/routers"
	"github.com/pg-sharding/spqr/pkg/models/shrule"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

type EntityMgr interface {
	kr.KeyRangeMgr
	shrule.ShardingRulesMgr
	routers.RouterMgr
	datashards.ShardsManager
}

var unknownCoordinatorCommand = fmt.Errorf("unknown coordinator cmd")

func processDrop(ctx context.Context, dstmt spqrparser.Statement, mngr EntityMgr, cli clientinteractor.PSQLInteractor, cl client.Client) error {
	switch stmt := dstmt.(type) {
	case *spqrparser.DropKeyRange:
		spqrlog.Logger.Printf(spqrlog.DEBUG2, "parsed drop %s to %s", stmt.KeyRangeID)
		err := mngr.DropKeyRange(ctx, stmt.KeyRangeID)
		if err != nil {
			return cli.ReportError(err, cl)
		}
		return cli.DropKeyRange(ctx, []string{stmt.KeyRangeID}, cl)
	case *spqrparser.DropShardingRule:
		spqrlog.Logger.Printf(spqrlog.DEBUG2, "parsed drop %s to %s", stmt.ID)
		err := mngr.DropShardingRule(ctx, stmt.ID)
		if err != nil {
			return cli.ReportError(err, cl)
		}
		return cli.DropShardingRule(ctx, stmt.ID, cl)
	default:
		return fmt.Errorf("unknown drop statement")
	}
}

func processAdd(ctx context.Context, astmt spqrparser.Statement, mngr EntityMgr, cli clientinteractor.PSQLInteractor, cl client.Client) error {
	switch stmt := astmt.(type) {
	case *spqrparser.AddShardingRule:
		shardingRule := shrule.NewShardingRule(stmt.ID, stmt.ColNames)
		err := mngr.AddShardingRule(ctx, shardingRule)
		if err != nil {
			return err
		}
		return cli.AddShardingRule(ctx, shardingRule, cl)
	case *spqrparser.AddKeyRange:
		req := kr.KeyRangeFromSQL(stmt)
		if err := mngr.AddKeyRange(ctx, req); err != nil {
			return cli.ReportError(err, cl)
		}
		return cli.AddKeyRange(ctx, req, cl)
	default:
		return unknownCoordinatorCommand
	}
}

func Proc(ctx context.Context, tstmt spqrparser.Statement, mgr EntityMgr, cli clientinteractor.PSQLInteractor, cl client.Client) error {
	switch stmt := tstmt.(type) {
	case *spqrparser.Drop:
		return processDrop(ctx, stmt.Element, mgr, cli, cl)
	case *spqrparser.Add:
		return processAdd(ctx, stmt.Element, mgr, cli, cl)
	case *spqrparser.DropAll:
		switch stmt.Entity {
		case spqrparser.EntityKeyRanges:
			if krids, err := mgr.DropAllKeyRanges(ctx); err != nil {
				return err
			} else {
				return cli.DropKeyRange(ctx, func() []string {
					var ret []string

					for _, krcurr := range krids {
						ret = append(ret, krcurr.ID)
					}

					return ret
				}(), cl)
			}
		case spqrparser.EntityShardingRule:
			if rules, err := mgr.DropAllShardingRules(ctx); err != nil {
				return err
			} else {
				return cli.DropShardingRule(ctx, func() string {
					var ret []string

					for _, rule := range rules {
						ret = append(ret, rule.ID())
					}

					return strings.Join(ret, ",")
				}(), cl)
			}
		case spqrparser.EntityRouters:
			return cli.ReportError(fmt.Errorf("unimplememnted"), cl)
		default:
			return fmt.Errorf("unknown entity to drop")
		}
	case *spqrparser.MoveKeyRange:
		move := &kr.MoveKeyRange{
			ShardId: stmt.DestShardID,
			Krid:    stmt.KeyRangeID,
		}

		if err := mgr.Move(ctx, move); err != nil {
			return cli.ReportError(err, cl)
		}

		return cli.MoveKeyRange(ctx, move, cl)

	case *spqrparser.RegisterRouter:
		newRouter := &routers.Router{
			Id:      stmt.ID,
			AdmAddr: stmt.Addr,
		}

		if err := mgr.RegisterRouter(ctx, newRouter); err != nil {
			return err
		}

		if err := mgr.SyncRouterMetadata(ctx, newRouter); err != nil {
			return err
		}

		return cli.RegisterRouter(ctx, cl, stmt.ID, stmt.Addr)
	case *spqrparser.UnregisterRouter:
		if err := mgr.UnregisterRouter(ctx, stmt.ID); err != nil {
			return err
		}
		return cli.UnregisterRouter(cl, stmt.ID)
	case *spqrparser.Lock:
		if _, err := mgr.LockKeyRange(ctx, stmt.KeyRangeID); err != nil {
			return err
		}
		return cli.LockKeyRange(ctx, stmt.KeyRangeID, cl)
	case *spqrparser.Unlock:
		if err := mgr.UnlockKeyRange(ctx, stmt.KeyRangeID); err != nil {
			return err
		}
		return cli.UnlockKeyRange(ctx, stmt.KeyRangeID, cl)
	case *spqrparser.Show:
		return ProcessShow(ctx, stmt, mgr, cli, cl)
	default:
		return unknownCoordinatorCommand
	}
}

func ProcessShow(ctx context.Context, stmt *spqrparser.Show, mngr EntityMgr, cli clientinteractor.PSQLInteractor, cl client.Client) error {
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
		return cli.Shards(ctx, resp, cl)
	case spqrparser.ShowKeyRangesStr:
		ranges, err := mngr.ListKeyRanges(ctx)
		if err != nil {
			return err
		}
		return cli.KeyRanges(ranges, cl)
	case spqrparser.ShowRoutersStr:
		resp, err := mngr.ListRouters(ctx)
		if err != nil {
			return err
		}

		return cli.Routers(resp, cl)
	case spqrparser.ShowShardingRules:
		resp, err := mngr.ListShardingRules(ctx)
		if err != nil {
			return err
		}

		return cli.ShardingRules(ctx, resp, cl)
	default:
		return unknownCoordinatorCommand
	}
}
