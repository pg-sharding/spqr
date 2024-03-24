package meta

import (
	"context"
	"fmt"
	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/clientinteractor"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/connectiterator"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/workloadlog"
	"github.com/pg-sharding/spqr/qdb"

	"github.com/pg-sharding/spqr/pkg/models/datashards"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

type EntityMgr interface {
	kr.KeyRangeMgr
	topology.RouterMgr
	datashards.ShardsMgr
	distributions.DistributionMgr

	ShareKeyRange(id string) error

	QDB() qdb.QDB
}

var unknownCoordinatorCommand = fmt.Errorf("unknown coordinator cmd")

// TODO : unit tests
func processDrop(ctx context.Context, dstmt spqrparser.Statement, isCascade bool, mngr EntityMgr, cli *clientinteractor.PSQLInteractor) error {
	switch stmt := dstmt.(type) {
	case *spqrparser.KeyRangeSelector:
		if stmt.KeyRangeID == "*" {
			if err := mngr.DropKeyRangeAll(ctx); err != nil {
				return cli.ReportError(err)
			} else {
				return cli.DropKeyRange(ctx, []string{})
			}
		} else {
			spqrlog.Zero.Debug().Str("kr", stmt.KeyRangeID).Msg("parsed drop")
			err := mngr.DropKeyRange(ctx, stmt.KeyRangeID)
			if err != nil {
				return cli.ReportError(err)
			}
			return cli.DropKeyRange(ctx, []string{stmt.KeyRangeID})
		}
	case *spqrparser.ShardingRuleSelector:
		return cli.ReportError(spqrerror.ShardingKeysRemoved)
	case *spqrparser.DistributionSelector:
		var krs []*kr.KeyRange
		var err error

		if stmt.ID == "*" {

			krs, err = mngr.ListAllKeyRanges(ctx)
			if err != nil {
				return err
			}
		} else {

			krs, err = mngr.ListKeyRanges(ctx, stmt.ID)
			if err != nil {
				return err
			}
		}

		if len(krs) != 0 && !isCascade {
			return fmt.Errorf("cannot drop distribution %s because other objects depend on it\nHINT: Use DROP ... CASCADE to drop the dependent objects too.", stmt.ID)
		}

		for _, kr := range krs {
			err = mngr.DropKeyRange(ctx, kr.ID)
			if err != nil {
				return err
			}
		}

		if stmt.ID != "*" {
			ds, err := mngr.GetDistribution(ctx, stmt.ID)
			if err != nil {
				return err
			}
			if len(ds.Relations) != 0 && !isCascade {
				return fmt.Errorf("cannot drop distribution %s because there are relations attached to it\nHINT: Use DROP ... CASCADE to detach relations automatically.", stmt.ID)
			}

			for _, rel := range ds.Relations {
				if err := mngr.AlterDistributionDetach(ctx, ds.Id, rel.Name); err != nil {
					return err
				}
			}
			if err := mngr.DropDistribution(ctx, stmt.ID); err != nil {
				return cli.ReportError(err)
			}
			return cli.DropDistribution(ctx, []string{stmt.ID})
		}

		dss, err := mngr.ListDistributions(ctx)
		if err != nil {
			return err
		}
		ret := make([]string, 0)
		if err != nil {
			return err
		}
		for _, ds := range dss {
			if ds.Id != "default" {
				if len(ds.Relations) != 0 && !isCascade {
					return fmt.Errorf("cannot drop distribution %s because there are relations attached to it\nHINT: Use DROP ... CASCADE to detach relations autoimatically", ds.Id)
				}
				ret = append(ret, ds.ID())
				err = mngr.DropDistribution(ctx, ds.Id)
				if err != nil {
					return err
				}
			}
		}

		return cli.DropDistribution(ctx, ret)
	case *spqrparser.ShardSelector:
		if err := mngr.DropShard(ctx, stmt.ID); err != nil {
			return err
		}
		return cli.DropShard(stmt.ID)
	default:
		return fmt.Errorf("unknown drop statement")
	}
}

// TODO : unit tests
func processCreate(ctx context.Context, astmt spqrparser.Statement, mngr EntityMgr, cli *clientinteractor.PSQLInteractor) error {
	switch stmt := astmt.(type) {
	case *spqrparser.DistributionDefinition:
		distribution := distributions.NewDistribution(stmt.ID, stmt.ColTypes)

		distributions, err := mngr.ListDistributions(ctx)
		if err != nil {
			return err
		}
		for _, ds := range distributions {
			if ds.Id == distribution.Id {
				spqrlog.Zero.Debug().Msg("Attempt to create existing distribution")
				return fmt.Errorf("attempt to create existing distribution")
			}
		}

		err = mngr.CreateDistribution(ctx, distribution)
		if err != nil {
			return err
		}
		return cli.AddDistribution(ctx, distribution)
	case *spqrparser.ShardingRuleDefinition:
		return cli.ReportError(spqrerror.ShardingKeysRemoved)
	case *spqrparser.KeyRangeDefinition:
		req := kr.KeyRangeFromSQL(stmt)
		if err := mngr.AddKeyRange(ctx, req); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("Error when adding key range")
			return cli.ReportError(err)
		}
		return cli.AddKeyRange(ctx, req)
	case *spqrparser.ShardDefinition:
		dataShard := datashards.NewDataShard(stmt.Id, &config.Shard{
			Hosts: stmt.Hosts,
			Type:  config.DataShard,
		})
		if err := mngr.AddDataShard(ctx, dataShard); err != nil {
			return err
		}
		return cli.AddShard(dataShard)
	default:
		return unknownCoordinatorCommand
	}
}

func processAlter(ctx context.Context, astmt spqrparser.Statement, mngr EntityMgr, cli *clientinteractor.PSQLInteractor) error {
	switch stmt := astmt.(type) {
	case *spqrparser.AlterDistribution:
		return processAlterDistribution(ctx, stmt.Element, mngr, cli)
	default:
		return unknownCoordinatorCommand
	}
}

func processAlterDistribution(ctx context.Context, astmt spqrparser.Statement, mngr EntityMgr, cli *clientinteractor.PSQLInteractor) error {
	switch stmt := astmt.(type) {
	case *spqrparser.AttachRelation:
		rels := []*distributions.DistributedRelation{}

		for _, drel := range stmt.Relations {
			rels = append(rels, distributions.DistributedRelationFromSQL(drel))
		}

		if err := mngr.AlterDistributionAttach(ctx, stmt.Distribution.ID, rels); err != nil {
			return err
		}
		return cli.AlterDistributionAttach(ctx, stmt.Distribution.ID, rels)
	case *spqrparser.DetachRelation:
		if err := mngr.AlterDistributionDetach(ctx, stmt.Distribution.ID, stmt.RelationName); err != nil {
			return err
		}
		return cli.AlterDistributionDetach(ctx, stmt.Distribution.ID, stmt.RelationName)
	default:
		return unknownCoordinatorCommand
	}
}

// TODO : unit tests
func Proc(ctx context.Context, tstmt spqrparser.Statement, mgr EntityMgr, ci connectiterator.ConnectIterator, cli *clientinteractor.PSQLInteractor, writer workloadlog.WorkloadLog) error {
	spqrlog.Zero.Debug().Interface("tstmt", tstmt).Msg("proc query")
	switch stmt := tstmt.(type) {
	case *spqrparser.TraceStmt:
		if writer == nil {
			return fmt.Errorf("can not save workload from here")
		}
		writer.StartLogging(stmt.All, stmt.Client)
		return cli.StartTraceMessages(ctx)
	case *spqrparser.StopTraceStmt:
		if writer == nil {
			return fmt.Errorf("can not save workload from here")
		}
		err := writer.StopLogging()
		if err != nil {
			return err
		}
		return cli.StopTraceMessages(ctx)
	case *spqrparser.Drop:
		return processDrop(ctx, stmt.Element, stmt.CascadeDelete, mgr, cli)
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
		if err := mgr.UnlockKeyRange(ctx, stmt.KeyRangeID); err != nil {
			return err
		}
		return cli.UnlockKeyRange(ctx, stmt.KeyRangeID)
	case *spqrparser.Show:
		return ProcessShow(ctx, stmt, mgr, ci, cli)
	case *spqrparser.Kill:
		return ProcessKill(ctx, stmt, mgr, ci, cli)
	case *spqrparser.SplitKeyRange:
		splitKeyRange := &kr.SplitKeyRange{
			Bound:    stmt.Border,
			SourceID: stmt.KeyRangeFromID,
			Krid:     stmt.KeyRangeID,
		}
		if err := mgr.Split(ctx, splitKeyRange); err != nil {
			return err
		}
		return cli.SplitKeyRange(ctx, splitKeyRange)
	case *spqrparser.UniteKeyRange:
		uniteKeyRange := &kr.UniteKeyRange{
			BaseKeyRangeId:      stmt.KeyRangeIDL,
			AppendageKeyRangeId: stmt.KeyRangeIDR,
		}
		if err := mgr.Unite(ctx, uniteKeyRange); err != nil {
			return err
		}
		return cli.MergeKeyRanges(ctx, uniteKeyRange)
	case *spqrparser.Alter:
		return processAlter(ctx, stmt.Element, mgr, cli)
	default:
		return unknownCoordinatorCommand
	}
}

// TODO : unit tests
func ProcessKill(ctx context.Context, stmt *spqrparser.Kill, mngr EntityMgr, pool client.Pool, cli *clientinteractor.PSQLInteractor) error {
	spqrlog.Zero.Debug().Str("cmd", stmt.Cmd).Msg("process kill")
	switch stmt.Cmd {
	case spqrparser.ClientStr:
		ok, err := pool.Pop(stmt.Target)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("No such client %d", stmt.Target)
		}
		return cli.KillClient(stmt.Target)
	default:
		return unknownCoordinatorCommand
	}
}

// TODO : unit tests
func ProcessShow(ctx context.Context, stmt *spqrparser.Show, mngr EntityMgr, ci connectiterator.ConnectIterator, cli *clientinteractor.PSQLInteractor) error {
	spqrlog.Zero.Debug().Str("cmd", stmt.Cmd).Msg("process show statement")
	switch stmt.Cmd {
	case spqrparser.BackendConnectionsStr:

		var resp []shard.Shardinfo
		if err := ci.ForEach(func(sh shard.Shardinfo) error {
			resp = append(resp, sh)
			return nil
		}); err != nil {
			return err
		}

		return cli.BackendConnections(ctx, resp)
	case spqrparser.ShardsStr:
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
	case spqrparser.KeyRangesStr:
		ranges, err := mngr.ListAllKeyRanges(ctx)
		if err != nil {
			return err
		}
		return cli.KeyRanges(ranges)
	case spqrparser.RoutersStr:
		resp, err := mngr.ListRouters(ctx)
		if err != nil {
			return err
		}

		return cli.Routers(resp)
	case spqrparser.ShardingRules:
		return cli.ReportError(spqrerror.ShardingKeysRemoved)
	case spqrparser.ClientsStr:
		var resp []client.ClientInfo
		if err := ci.ClientPoolForeach(func(client client.ClientInfo) error {
			resp = append(resp, client)
			return nil
		}); err != nil {
			return err
		}

		return cli.Clients(ctx, resp, stmt.Where)
	case spqrparser.PoolsStr:
		var respPools []pool.Pool
		if err := ci.ForEachPool(func(p pool.Pool) error {
			respPools = append(respPools, p)
			return nil
		}); err != nil {
			return err
		}

		return cli.Pools(ctx, respPools)
	case spqrparser.VersionStr:
		return cli.Version(ctx)
	case spqrparser.DistributionsStr:
		dss, err := mngr.ListDistributions(ctx)
		if err != nil {
			return err
		}
		return cli.Distributions(ctx, dss)
	case spqrparser.RelationsStr:
		dss, err := mngr.ListDistributions(ctx)
		if err != nil {
			return err
		}
		dsToRels := make(map[string][]*distributions.DistributedRelation)
		for _, ds := range dss {
			if _, ok := dsToRels[ds.Id]; ok {
				return spqrerror.Newf(spqrerror.SPQR_METADATA_CORRUPTION, "Duplicate values on \"%s\" distribution ID", ds.Id)
			}
			dsToRels[ds.Id] = make([]*distributions.DistributedRelation, 0)
			for _, rel := range ds.Relations {
				dsToRels[ds.Id] = append(dsToRels[ds.Id], rel)
			}
		}

		return cli.Relations(dsToRels, stmt.Where)
	default:
		return unknownCoordinatorCommand
	}
}
