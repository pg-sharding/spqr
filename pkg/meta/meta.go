package meta

import (
	"context"
	"fmt"

	"github.com/pg-sharding/spqr/pkg/catalog"
	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/clientinteractor"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/connectiterator"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/models/tasks"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/workloadlog"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/cache"
	rclient "github.com/pg-sharding/spqr/router/client"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

const (
	defaultBatchSize = 500
)

type EntityMgr interface {
	kr.KeyRangeMgr
	topology.RouterMgr
	topology.ShardsMgr
	distributions.DistributionMgr
	tasks.TaskMgr

	ShareKeyRange(id string) error

	QDB() qdb.QDB
	Cache() *cache.SchemaCache
}

var unknownCoordinatorCommand = fmt.Errorf("unknown coordinator cmd")

// TODO : unit tests

// processDrop processes the drop command based on the type of statement provided.
//
// Parameters:
// - ctx (context.Context): The context for the request.
// - dstmt (spqrparser.Statement): The statement to be processed.
// - isCascade (bool): Indicates whether cascade is enabled.
// - mngr (EntityMgr): The entity manager handling the drop operation.
// - cli (*clientinteractor.PSQLInteractor): The PSQL interactor for reporting errors.
//
// Returns:
// - error: An error if drop operation fails, otherwise nil.
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
		return cli.ReportError(spqrerror.ShardingRulesRemoved)
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
		for _, ds := range dss {
			if ds.Id != "default" {
				if len(ds.Relations) != 0 && !isCascade {
					return fmt.Errorf("cannot drop distribution %s because there are relations attached to it\nHINT: Use DROP ... CASCADE to detach relations automatically", ds.Id)
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
	case *spqrparser.TaskGroupSelector:
		if err := mngr.RemoveMoveTaskGroup(ctx); err != nil {
			return err
		}
		return cli.DropTaskGroup(ctx)
	default:
		return fmt.Errorf("unknown drop statement")
	}
}

// TODO : unit tests

// processCreate processes the given astmt statement of type spqrparser.Statement by creating a new distribution,
// sharding rule, key range, or data shard depending on the type of the statement.
//
// Parameters:
// - ctx (context.Context): The context of the operation.
// - astmt (spqrparser.Statement): The statement to be processed.
// - mngr (EntityMgr): The entity manager used to manage the entities.
// - cli (*clientinteractor.PSQLInteractor): The PSQL interactor used to interact with the PSQL client.
//
// Returns:
// - error: An error if the creation encounters any issues.
func processCreate(ctx context.Context, astmt spqrparser.Statement, mngr EntityMgr, cli *clientinteractor.PSQLInteractor) error {
	switch stmt := astmt.(type) {
	case *spqrparser.ReferenceRelationDefinition:

		selectedDistribId := distributions.REPLICATED

		if _, err := mngr.GetDistribution(ctx, selectedDistribId); err != nil {
			err := mngr.CreateDistribution(ctx, &distributions.Distribution{
				Id:       distributions.REPLICATED,
				ColTypes: nil,
			})
			if err != nil {
				spqrlog.Zero.Debug().Err(err).Msg("failed to setup REPLICATED distribution")
				return cli.ReportError(err)
			}
		}

		rels := []*distributions.DistributedRelation{
			{
				Name:               stmt.TableName,
				ReplicatedRelation: true,
			},
		}

		if err := mngr.AlterDistributionAttach(ctx, selectedDistribId, rels); err != nil {
			return cli.ReportError(err)
		}

		return cli.AlterDistributionAttach(ctx, selectedDistribId, rels)

	case *spqrparser.DistributionDefinition:
		if stmt.ID == "default" {
			return spqrerror.New(spqrerror.SPQR_INVALID_REQUEST, "You cannot create a \"default\" distribution, \"default\" is a reserved word")
		}
		var distribution *distributions.Distribution

		if stmt.Replicated {
			if _, err := mngr.GetDistribution(ctx, distributions.REPLICATED); err != nil {
				distribution = &distributions.Distribution{
					Id:       distributions.REPLICATED,
					ColTypes: nil,
				}
				err := mngr.CreateDistribution(ctx, distribution)
				if err != nil {
					spqrlog.Zero.Debug().Err(err).Msg("failed to setup REPLICATED distribution")
					return cli.ReportError(err)
				}
			} else {
				return fmt.Errorf("REPLICATED distribution already exist.")
			}
		} else {
			distribution = distributions.NewDistribution(stmt.ID, stmt.ColTypes)

			dds, err := mngr.ListDistributions(ctx)
			if err != nil {
				return err
			}
			for _, ds := range dds {
				if ds.Id == distribution.Id {
					spqrlog.Zero.Debug().Msg("Attempt to create existing distribution")
					return fmt.Errorf("attempt to create existing distribution")
				}
			}

			err = mngr.CreateDistribution(ctx, distribution)
			if err != nil {
				return err
			}
		}
		return cli.AddDistribution(ctx, distribution)
	case *spqrparser.ShardingRuleDefinition:
		return cli.ReportError(spqrerror.ShardingRulesRemoved)
	case *spqrparser.KeyRangeDefinition:
		if stmt.Distribution == "default" {
			list, err := mngr.ListDistributions(ctx)
			if err != nil {
				return spqrerror.New(spqrerror.SPQR_NO_DISTRIBUTION, "error while selecting list of distributions")
			}
			if len(list) == 0 {
				return spqrerror.New(spqrerror.SPQR_NO_DISTRIBUTION, "you don't have any distributions")
			}
			if len(list) > 1 {
				return spqrerror.New(spqrerror.SPQR_NO_DISTRIBUTION, "distributions count not equal one, use FOR DISTRIBUTION syntax")
			}
			stmt.Distribution = list[0].Id
		}
		ds, err := mngr.GetDistribution(ctx, stmt.Distribution)
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("Error when adding key range")
			return cli.ReportError(err)
		}
		req, err := kr.KeyRangeFromSQL(stmt, ds.ColTypes)
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("Error when adding key range")
			return cli.ReportError(err)
		}
		if err := mngr.CreateKeyRange(ctx, req); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("Error when adding key range")
			return cli.ReportError(err)
		}
		return cli.CreateKeyRange(ctx, req)
	case *spqrparser.ShardDefinition:
		dataShard := topology.NewDataShard(stmt.Id, &config.Shard{
			RawHosts: stmt.Hosts,
			Type:     config.DataShard,
		})
		if err := mngr.AddDataShard(ctx, dataShard); err != nil {
			return err
		}
		return cli.AddShard(dataShard)
	default:
		return unknownCoordinatorCommand
	}
}

// processAlter processes the given alter statement and performs the corresponding operation.
//
// Parameters:
// - ctx (context.Context): The context for the operation.
// - astmt (spqrparser.Statement): The alter statement to be processed.
// - mngr (EntityMgr): The entity manager for managing entities.
// - cli (*clientinteractor.PSQLInteractor): The PSQL client interactor for interacting with the PSQL server.
//
// Returns:
// - error: An error if the operation fails, otherwise nil.
func processAlter(ctx context.Context, astmt spqrparser.Statement, mngr EntityMgr, cli *clientinteractor.PSQLInteractor) error {
	switch stmt := astmt.(type) {
	case *spqrparser.AlterDistribution:
		return processAlterDistribution(ctx, stmt.Element, mngr, cli)
	default:
		return unknownCoordinatorCommand
	}
}

// processAlterDistribution processes the given alter distribution statement and performs the corresponding operation.
//
// Parameters:
// - ctx (context.Context): The context for the operation.
// - astmt (spqrparser.Statement): The alter distribution statement to be processed.
// - mngr (EntityMgr): The entity manager for managing entities.
// - cli (*clientinteractor.PSQLInteractor): The PSQL client interactor for interacting with the PSQL server.
//
// Returns:
// - error: An error if the operation fails, otherwise nil.
func processAlterDistribution(ctx context.Context, astmt spqrparser.Statement, mngr EntityMgr, cli *clientinteractor.PSQLInteractor) error {
	switch stmt := astmt.(type) {
	case *spqrparser.AttachRelation:
		rels := []*distributions.DistributedRelation{}
		for _, drel := range stmt.Relations {
			if stmt.Distribution.Replicated && !drel.ReplicatedRelation {

				return spqrerror.New(spqrerror.SPQR_NO_DISTRIBUTION, "non replicated relation should be attached to non replicated distribution")
			}

			ds, err := mngr.GetDistribution(ctx, stmt.Distribution.ID)
			if err != nil {
				return cli.ReportError(err)
			}

			for _, entry := range drel.DistributionKey {
				if entry.HashFunction == "city" {
					for _, colType := range ds.ColTypes {
						if colType == "varchar" {
							//ds.ColTypes[i] = "varchar hash"
							return fmt.Errorf("hash function CITY is incompatible with column type varchar. HINT: Change column type to 'varchar hash' for CITY hash function")
						}
					}
				}
			}

			rels = append(rels, distributions.DistributedRelationFromSQL(drel))
		}
		var selectedDistribId string

		if stmt.Distribution.Replicated {
			selectedDistribId = distributions.REPLICATED

			if _, err := mngr.GetDistribution(ctx, selectedDistribId); err != nil {
				err := mngr.CreateDistribution(ctx, &distributions.Distribution{
					Id:       distributions.REPLICATED,
					ColTypes: nil,
				})
				if err != nil {
					spqrlog.Zero.Debug().Err(err).Msg("failed to setup REPLICATED distribution")
					return cli.ReportError(err)
				}
			}
		} else {
			selectedDistribId = stmt.Distribution.ID
		}

		if err := mngr.AlterDistributionAttach(ctx, selectedDistribId, rels); err != nil {
			return cli.ReportError(err)
		}

		return cli.AlterDistributionAttach(ctx, selectedDistribId, rels)

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

// Proc processes various coordinator commands based on the provided statement.
//
// Parameters:
// - ctx (context.Context): The context for the function.
// - tstmt (spqrparser.Statement): The statement to be processed.
// - mgr (EntityMgr): The entity manager.
// - ci (connectiterator.ConnectIterator): The connect iterator.
// - cli (*clientinteractor.PSQLInteractor): The PSQL interactor.
// - writer (workloadlog.WorkloadLog): The workload log writer.
//
// Returns:
// - error: An error if the operation fails, otherwise nil.
func Proc(ctx context.Context, tstmt spqrparser.Statement, mgr EntityMgr, ci connectiterator.ConnectIterator, rc rclient.RouterClient, writer workloadlog.WorkloadLog) error {
	cli := clientinteractor.NewPSQLInteractor(rc)
	spqrlog.Zero.Debug().Interface("tstmt", tstmt).Msg("proc query")

	if _, ok := tstmt.(*spqrparser.Show); ok {
		if err := catalog.GC.CheckGrants(catalog.RoleReader, rc.Rule()); err != nil {
			return err
		}
		return ProcessShow(ctx, tstmt.(*spqrparser.Show), mgr, ci, cli)
	}

	if err := catalog.GC.CheckGrants(catalog.RoleAdmin, rc.Rule()); err != nil {
		return err
	}

	switch stmt := tstmt.(type) {
	case *spqrparser.TraceStmt:
		if writer == nil {
			return fmt.Errorf("cannot save workload from here")
		}
		writer.StartLogging(stmt.All, stmt.Client)
		return cli.StartTraceMessages(ctx)
	case *spqrparser.StopTraceStmt:
		if writer == nil {
			return fmt.Errorf("cannot save workload from here")
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
	case *spqrparser.Kill:
		return ProcessKill(ctx, stmt, mgr, ci, cli)
	case *spqrparser.SplitKeyRange:
		splitKeyRange := &kr.SplitKeyRange{
			Bound:    stmt.Border.Pivots,
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
	case *spqrparser.RedistributeKeyRange:
		return processRedistribute(ctx, stmt, mgr, cli)
	case *spqrparser.InvalidateCache:
		mgr.Cache().Reset()
		return cli.CompleteMsg(0)
	default:
		return unknownCoordinatorCommand
	}
}

// TODO : unit tests

// ProcessKill processes the kill command based on the statement provided.
//
// Parameters:
// - ctx (context.Context): The context for the operation.
// - stmt (*spqrparser.Kill): The kill statement to process.
// - mngr (EntityMgr): The entity manager for managing entities.
// - pool (client.Pool): The pool of clients.
// - cli (*clientinteractor.PSQLInteractor): The PSQL interactor for client interactions.
//
// Returns:
// - error: An error if the operation encounters any issues.
func ProcessKill(ctx context.Context, stmt *spqrparser.Kill, mngr EntityMgr, pool client.Pool, cli *clientinteractor.PSQLInteractor) error {
	spqrlog.Zero.Debug().Str("cmd", stmt.Cmd).Msg("process kill")
	switch stmt.Cmd {
	case spqrparser.ClientStr:
		ok, err := pool.Pop(stmt.Target)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("no such client %d", stmt.Target)
		}
		return cli.KillClient(stmt.Target)
	default:
		return unknownCoordinatorCommand
	}
}

// TODO : unit tests

// ProcessShow processes the SHOW statement and returns an error if any issue occurs.
//
// Parameters:
// - ctx (context.Context): The context for the operation.
// - stmt (*spqrparser.Show): The SHOW statement to process.
// - mngr (EntityMgr): The entity manager for managing entities.
// - ci (connectiterator.ConnectIterator): The connect iterator for connection interactions.
// - cli (*clientinteractor.PSQLInteractor): The PSQL interactor for client interactions.
//
// Returns:
// - error: An error if the operation encounters any issues.
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

		return cli.BackendConnections(ctx, resp, stmt)
	case spqrparser.ShardsStr:
		shards, err := mngr.ListShards(ctx)
		if err != nil {
			return err
		}
		var resp []*topology.DataShard
		for _, sh := range shards {
			resp = append(resp, &topology.DataShard{
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
		return cli.ReportError(spqrerror.ShardingRulesRemoved)
	case spqrparser.ClientsStr:
		var resp []client.ClientInfo
		if err := ci.ClientPoolForeach(func(client client.ClientInfo) error {
			resp = append(resp, client)
			return nil
		}); err != nil {
			return err
		}

		return cli.Clients(ctx, resp, stmt)
	case spqrparser.PoolsStr:
		var respPools []pool.Pool
		if err := ci.ForEachPool(func(p pool.Pool) error {
			respPools = append(respPools, p)
			return nil
		}); err != nil {
			return err
		}

		return cli.Pools(ctx, respPools)
	case spqrparser.InstanceStr:
		return cli.Instance(ctx, ci)
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
	case spqrparser.TaskGroupStr:
		group, err := mngr.GetMoveTaskGroup(ctx)
		if err != nil {
			return err
		}
		return cli.MoveTaskGroup(ctx, group)
	case spqrparser.PreparedStatementsStr:

		var resp []shard.PreparedStatementsMgrDescriptor
		if err := ci.ForEach(func(sh shard.Shardinfo) error {
			resp = append(resp, sh.ListPreparedStatements()...)
			return nil
		}); err != nil {
			return err
		}

		return cli.PreparedStatements(ctx, resp)
	case spqrparser.QuantilesStr:
		return cli.Quantiles(ctx)
	default:
		return unknownCoordinatorCommand
	}
}

// TODO : unit tests

// processRedistribute processes the REDISTRIBUTE KEY RANGE statement and returns an error if any issue occurs.
//
// Parameters:
//   - ctx (context.Context): The context for the operation.
//   - stmt (*spqrparser.Show): The REDISTRIBUTE KEY RANGE statement to process.
//   - mngr (EntityMgr): The entity manager for performing the redistribution.
//   - cli (*clientinteractor.PSQLInteractor): The PSQL interactor for client interactions.
//
// Returns:
// - error: An error if the operation encounters any issues.
func processRedistribute(ctx context.Context, req *spqrparser.RedistributeKeyRange, mngr EntityMgr, cli *clientinteractor.PSQLInteractor) error {
	spqrlog.Zero.Debug().Str("key range id", req.KeyRangeID).Str("destination shard id", req.DestShardID).Int("batch size", req.BatchSize).Msg("process redistribute")
	if req.BatchSize <= 0 {
		spqrlog.Zero.Debug().
			Int("batch-size-got", req.BatchSize).
			Int("batch-size-use", defaultBatchSize).
			Msg("redistribute: using default batch size")
		req.BatchSize = defaultBatchSize
	}
	if err := mngr.RedistributeKeyRange(ctx, &kr.RedistributeKeyRange{
		KrId:      req.KeyRangeID,
		ShardId:   req.DestShardID,
		BatchSize: req.BatchSize,
		Check:     req.Check,
	}); err != nil {
		return cli.ReportError(err)
	}
	return cli.RedistributeKeyRange(ctx, req)
}
