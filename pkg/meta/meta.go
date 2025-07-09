package meta

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/pg-sharding/spqr/coordinator/statistics"
	"github.com/pg-sharding/spqr/pkg/catalog"
	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/clientinteractor"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/connmgr"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/rrelation"
	"github.com/pg-sharding/spqr/pkg/models/sequences"
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
	"github.com/pg-sharding/spqr/router/rfqn"
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
	sequences.SequenceMgr
	rrelation.ReferenceRelationMgr

	ShareKeyRange(id string) error

	QDB() qdb.QDB
	Cache() *cache.SchemaCache
}

var ErrUnknownCoordinatorCommand = fmt.Errorf("unknown coordinator cmd")

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
	case *spqrparser.ReferenceRelationSelector:
		/* XXX: fix reference relation selector to support schema-qualified names */
		relName := &rfqn.RelationFQN{
			RelationName: stmt.ID,
		}
		if err := mngr.DropReferenceRelation(ctx, relName); err != nil {
			return err
		}
		return cli.DropReferenceRelation(ctx, stmt.ID)
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
			return fmt.Errorf("cannot drop distribution %s because other objects depend on it\nHINT: Use DROP ... CASCADE to drop the dependent objects too.", stmt.ID) //nolint:staticcheck
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
				return fmt.Errorf("cannot drop distribution %s because there are relations attached to it\nHINT: Use DROP ... CASCADE to detach relations automatically.", stmt.ID) //nolint:staticcheck
			}

			for _, rel := range ds.Relations {
				qualifiedName := &rfqn.RelationFQN{RelationName: rel.Name, SchemaName: rel.SchemaName}
				if err := mngr.AlterDistributionDetach(ctx, ds.Id, qualifiedName); err != nil {
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
					return fmt.Errorf("cannot drop distribution %s because there are relations attached to it\nHINT: Use DROP ... CASCADE to detach relations automatically.", ds.Id) //nolint:staticcheck
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
	case *spqrparser.SequenceSelector:
		if err := mngr.DropSequence(ctx, stmt.Name); err != nil {
			return err
		}
		return cli.DropSequence(ctx, stmt.Name)
	default:
		return fmt.Errorf("unknown drop statement")
	}
}

// TODO : unit tests

// createReplicatedDistribution creates replicated distribution
//
// Parameters:
// - ctx (context.Context): The context of the operation.
// - mngr (EntityMgr): The entity manager used to manage the entities.
// Returns:
// - distribution: created distribution.
// - error: An error if the creation encounters any issues.
func createReplicatedDistribution(ctx context.Context, mngr EntityMgr) (*distributions.Distribution, error) {
	if _, err := mngr.GetDistribution(ctx, distributions.REPLICATED); err != nil {
		distribution := &distributions.Distribution{
			Id:       distributions.REPLICATED,
			ColTypes: nil,
		}
		err := mngr.CreateDistribution(ctx, distribution)
		if err != nil {
			spqrlog.Zero.Debug().Err(err).Msg("failed to setup REPLICATED distribution")
			return nil, err
		}
		return distribution, nil
	} else {
		return nil, fmt.Errorf("REPLICATED distribution already exist")
	}
}

// TODO : unit tests

// createNonReplicatedDistribution creates non replicated distribution
//
// Parameters:
// - ctx (context.Context): The context of the operation.
// - stmt (spqrparser.DistributionDefinition): The create distribution statement to be processed.
// - mngr (EntityMgr): The entity manager used to manage the entities.
// Returns:
// - distribution: created distribution.
// - error: An error if the creation encounters any issues.
func createNonReplicatedDistribution(ctx context.Context,
	stmt spqrparser.DistributionDefinition,
	mngr EntityMgr) (*distributions.Distribution, error) {
	distribution := distributions.NewDistribution(stmt.ID, stmt.ColTypes)

	dds, err := mngr.ListDistributions(ctx)
	if err != nil {
		return nil, err
	}
	var defaultShard *topology.DataShard
	if stmt.DefaultShard != "" {
		if ds, err := mngr.GetShard(ctx, stmt.DefaultShard); err != nil {
			return nil, fmt.Errorf("shard '%s' does not exist", stmt.DefaultShard)
		} else {
			defaultShard = ds
		}
	}

	for _, ds := range dds {
		if ds.Id == distribution.Id {
			spqrlog.Zero.Debug().Msg("Attempt to create existing distribution")
			return nil, fmt.Errorf("attempt to create existing distribution")
		}
	}

	err = mngr.CreateDistribution(ctx, distribution)
	if err != nil {
		return nil, err
	}
	if defaultShard != nil {
		defaultShardManager := NewDefaultShardManager(distribution, mngr)
		if defShardRes := defaultShardManager.CreateDefaultShardNoCheck(ctx, defaultShard); defShardRes != nil {
			return nil, fmt.Errorf("distribution %s created, but keyrange not. Error: %s",
				distribution.Id, defShardRes.Error())
		}
	}

	return distribution, nil
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

		r := &rrelation.ReferenceRelation{
			TableName:     stmt.TableName,
			SchemaVersion: 1,
			ShardId:       stmt.ShardIds,
		}

		if err := mngr.CreateReferenceRelation(ctx, r, rrelation.ReferenceRelationEntriesFromSQL(stmt.AutoIncrementEntries)); err != nil {
			return cli.ReportError(err)
		}

		return cli.CreateReferenceRelation(ctx, r)

	case *spqrparser.DistributionDefinition:
		if stmt.ID == "default" {
			return spqrerror.New(spqrerror.SPQR_INVALID_REQUEST, "You cannot create a \"default\" distribution, \"default\" is a reserved word")
		}
		if stmt.Replicated {
			if distribution, err := createReplicatedDistribution(ctx, mngr); err != nil {
				return cli.ReportError(err)
			} else {
				return cli.AddDistribution(ctx, distribution)
			}
		} else {
			distribution, createError := createNonReplicatedDistribution(ctx, *stmt, mngr)
			if createError != nil {
				return cli.ReportError(createError)
			} else {
				return cli.AddDistribution(ctx, distribution)
			}
		}
	case *spqrparser.ShardingRuleDefinition:
		return cli.ReportError(spqrerror.ShardingRulesRemoved)
	case *spqrparser.KeyRangeDefinition:
		if stmt.Distribution == "default" {
			list, err := mngr.ListDistributions(ctx)
			if err != nil {
				return spqrerror.New(spqrerror.SPQR_OBJECT_NOT_EXIST, "error while selecting list of distributions")
			}
			if len(list) == 0 {
				return spqrerror.New(spqrerror.SPQR_OBJECT_NOT_EXIST, "you don't have any distributions")
			}
			if len(list) > 1 {
				return spqrerror.New(spqrerror.SPQR_OBJECT_NOT_EXIST, "distributions count not equal one, use FOR DISTRIBUTION syntax")
			}
			stmt.Distribution = list[0].Id
		}
		ds, err := mngr.GetDistribution(ctx, stmt.Distribution)
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("Error when adding key range")
			return cli.ReportError(err)
		}
		if defaultKr := DefaultKeyRangeId(ds); stmt.KeyRangeID == defaultKr {
			errorStr := fmt.Sprintf("Error kay range %s is reserved", defaultKr)
			spqrlog.Zero.Error().Err(err).Msg(errorStr)
			return cli.ReportError(errors.New(errorStr))
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
		return ErrUnknownCoordinatorCommand
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
		return ErrUnknownCoordinatorCommand
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

			rels = append(rels, distributions.DistributedRelationFromSQL(drel))
		}

		selectedDistribId := stmt.Distribution.ID

		if err := mngr.AlterDistributionAttach(ctx, selectedDistribId, rels); err != nil {
			return cli.ReportError(err)
		}

		return cli.AlterDistributionAttach(ctx, selectedDistribId, rels)
	case *spqrparser.DetachRelation:
		if err := mngr.AlterDistributionDetach(ctx, stmt.Distribution.ID, stmt.RelationName); err != nil {
			return err
		}
		return cli.AlterDistributionDetach(ctx, stmt.Distribution.ID, stmt.RelationName.String())
	case *spqrparser.AlterRelation:
		if err := mngr.AlterDistributedRelation(ctx, stmt.Distribution.ID, distributions.DistributedRelationFromSQL(stmt.Relation)); err != nil {
			return err
		}
		qName := rfqn.RelationFQN{RelationName: stmt.Relation.Name, SchemaName: stmt.Relation.SchemaName}
		return cli.AlterDistributedRelation(ctx, stmt.Distribution.ID, qName.String())
	case *spqrparser.DropDefaultShard:
		if distribution, err := mngr.GetDistribution(ctx, stmt.Distribution.ID); err != nil {
			return err
		} else {
			manager := NewDefaultShardManager(distribution, mngr)
			if defaultShard, err := manager.DropDefaultShard(ctx); err != nil {
				return err
			} else {
				return cli.MakeSimpleResponse(ctx, manager.SuccessDropResponse(*defaultShard))
			}
		}
	case *spqrparser.AlterDefaultShard:
		if distribution, err := mngr.GetDistribution(ctx, stmt.Distribution.ID); err != nil {
			return err
		} else {
			manager := NewDefaultShardManager(distribution, mngr)
			if err := manager.CreateDefaultShard(ctx, stmt.Shard); err != nil {
				return err
			}
			return cli.MakeSimpleResponse(ctx, manager.SuccessCreateResponse(stmt.Shard))
		}
	default:
		return ErrUnknownCoordinatorCommand
	}
}

// TODO : unit tests

// ProcMetadataCommand processes various coordinator commands based on the provided statement.
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
func ProcMetadataCommand(ctx context.Context, tstmt spqrparser.Statement, mgr EntityMgr, ci connmgr.ConnectionStatsMgr, rc rclient.RouterClient, writer workloadlog.WorkloadLog, ro bool) error {
	cli := clientinteractor.NewPSQLInteractor(rc)
	spqrlog.Zero.Debug().Interface("tstmt", tstmt).Msg("proc query")

	if _, ok := tstmt.(*spqrparser.Show); ok {
		if err := catalog.GC.CheckGrants(catalog.RoleReader, rc.Rule()); err != nil {
			return err
		}
		return ProcessShow(ctx, tstmt.(*spqrparser.Show), mgr, ci, cli, ro)
	}

	if ro {
		return fmt.Errorf("console is in read only mode")
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
	case *spqrparser.RetryMoveTaskGroup:
		taskGroup, err := mgr.GetMoveTaskGroup(ctx)
		if err != nil {
			return err
		}
		colTypes := make([]string, 0)
		if len(taskGroup.Tasks) > 0 {
			kRange, err := mgr.GetKeyRange(ctx, taskGroup.KrIdFrom)
			if err != nil {
				return err
			}
			colTypes = kRange.ColumnTypes
		}
		if err = mgr.RetryMoveTaskGroup(ctx); err != nil {
			return err
		}
		return cli.MoveTaskGroup(ctx, taskGroup, colTypes)
	case *spqrparser.SyncReferenceTables:
		/* TODO: fix RelationSelector logic */
		if err := mgr.SyncReferenceRelations(ctx, []*rfqn.RelationFQN{
			{
				RelationName: stmt.RelationSelector},
		}, stmt.ShardID); err != nil {
			return err
		}
		return cli.SyncReferenceRelations([]string{stmt.RelationSelector}, stmt.ShardID)
	default:
		return ErrUnknownCoordinatorCommand
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
func ProcessKill(ctx context.Context, stmt *spqrparser.Kill, mngr EntityMgr, clPool client.Pool, cli *clientinteractor.PSQLInteractor) error {
	spqrlog.Zero.Debug().Str("cmd", stmt.Cmd).Msg("process kill")
	switch stmt.Cmd {
	case spqrparser.ClientStr:
		ok, err := clPool.Pop(stmt.Target)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("no such client %d", stmt.Target)
		}
		return cli.KillClient(stmt.Target)
	default:
		return ErrUnknownCoordinatorCommand
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
func ProcessShow(ctx context.Context, stmt *spqrparser.Show, mngr EntityMgr, ci connmgr.ConnectionStatsMgr, cli *clientinteractor.PSQLInteractor, ro bool) error {
	spqrlog.Zero.Debug().Str("cmd", stmt.Cmd).Msg("process show statement")
	switch stmt.Cmd {
	case spqrparser.BackendConnectionsStr:

		var resp []shard.ShardHostInfo
		if err := ci.ForEach(func(sh shard.ShardHostInfo) error {
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
		return cli.Shards(ctx, shards)
	case spqrparser.HostsStr:
		shards, err := mngr.ListShards(ctx)
		if err != nil {
			return err
		}

		ihc := ci.InstanceHealthChecks()

		return cli.Hosts(ctx, shards, ihc)
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
	case spqrparser.CoordinatorAddrStr:
		addr, err := mngr.GetCoordinator(ctx)
		if err != nil {
			return err
		}
		return cli.CoordinatorAddr(ctx, addr)
	case spqrparser.DistributionsStr:
		dss, err := mngr.ListDistributions(ctx)
		if err != nil {
			return err
		}
		var defShardIDs []string
		for _, d := range dss {
			krs, err := mngr.ListKeyRanges(ctx, d.Id)
			if err != nil {
				return err
			}

			shID := "not exists"
			/* XXX: yes, this is O(n), but "show distributions" is not high rpc either */
			for _, kr := range krs {
				if kr.ID == DefaultKeyRangeId(d) {
					shID = kr.ShardID
				}
			}

			defShardIDs = append(defShardIDs, shID)
		}
		return cli.Distributions(ctx, dss, defShardIDs)
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
	case spqrparser.ReferenceRelationsStr:
		rrs, err := mngr.ListReferenceRelations(ctx)
		if err != nil {
			return err
		}

		slices.SortFunc(rrs, func(a *rrelation.ReferenceRelation, b *rrelation.ReferenceRelation) int {
			if a.TableName == b.TableName {
				return 0
			}
			if a.TableName < b.TableName {
				return -1
			}
			return 1
		})

		return cli.ReferenceRelations(rrs)
	case spqrparser.TaskGroupStr:
		group, err := mngr.GetMoveTaskGroup(ctx)
		if err != nil {
			return err
		}
		colTypes := make([]string, 0)
		if len(group.Tasks) > 0 {
			kRange, err := mngr.GetKeyRange(ctx, group.KrIdFrom)
			if err != nil {
				return err
			}
			colTypes = kRange.ColumnTypes
		}

		return cli.MoveTaskGroup(ctx, group, colTypes)
	case spqrparser.PreparedStatementsStr:

		var resp []shard.PreparedStatementsMgrDescriptor
		if err := ci.ForEach(func(sh shard.ShardHostInfo) error {
			resp = append(resp, sh.ListPreparedStatements()...)
			return nil
		}); err != nil {
			return err
		}

		return cli.PreparedStatements(ctx, resp)
	case spqrparser.QuantilesStr:
		return cli.Quantiles(ctx)
	case spqrparser.SequencesStr:
		seqs, err := mngr.ListSequences(ctx)
		if err != nil {
			return err
		}

		/* this is a bit ugly, but it's quick and dirty.
		* a better approach would be to do it like we do for all other objects.
		 */

		var sequenceVals []int64

		for _, s := range seqs {
			v, err := mngr.CurrVal(ctx, s)
			if err != nil {
				return err
			}
			sequenceVals = append(sequenceVals, v)
		}

		return cli.Sequences(ctx, seqs, sequenceVals)
	case spqrparser.IsReadOnlyStr:
		return cli.IsReadOnly(ctx, ro)
	case spqrparser.MoveStatsStr:
		stats := statistics.GetMoveStats()
		return cli.MoveStats(ctx, stats)
	default:
		return ErrUnknownCoordinatorCommand
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
		Apply:     req.Apply,
	}); err != nil {
		return cli.ReportError(err)
	}
	return cli.RedistributeKeyRange(ctx, req)
}
