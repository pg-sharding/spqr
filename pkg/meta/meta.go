package meta

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/pg-sharding/spqr/coordinator/statistics"
	"github.com/pg-sharding/spqr/pkg/catalog"
	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/clientinteractor"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/connmgr"
	"github.com/pg-sharding/spqr/pkg/engine"
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
	"github.com/pg-sharding/spqr/pkg/tupleslot"
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
func processDrop(ctx context.Context,
	dstmt spqrparser.Statement,
	isCascade bool, mngr EntityMgr) (*tupleslot.TupleTableSlot, error) {
	switch stmt := dstmt.(type) {
	case *spqrparser.KeyRangeSelector:
		if stmt.KeyRangeID == "*" {
			krs, err := mngr.ListAllKeyRanges(ctx)
			if err != nil {
				return nil, err
			}
			if err := mngr.DropKeyRangeAll(ctx); err != nil {
				return nil, err
			} else {
				tts := &tupleslot.TupleTableSlot{}

				tts.Desc = engine.GetVPHeader("drop key range")

				for _, k := range krs {
					tts.Raw = append(tts.Raw, [][]byte{
						fmt.Appendf(nil, "key range id -> %s", k.ID),
					})
				}

				return tts, err
			}
		} else {
			spqrlog.Zero.Debug().Str("kr", stmt.KeyRangeID).Msg("parsed drop")
			err := mngr.DropKeyRange(ctx, stmt.KeyRangeID)
			if err != nil {
				return nil, err
			}

			tts := &tupleslot.TupleTableSlot{}

			tts.Desc = engine.GetVPHeader("drop key range")

			tts.Raw = append(tts.Raw, [][]byte{
				fmt.Appendf(nil, "key range id -> %s", stmt.KeyRangeID),
			})

			return tts, err
		}
	case *spqrparser.ShardingRuleSelector:
		return nil, spqrerror.ShardingRulesRemoved
	case *spqrparser.ReferenceRelationSelector:
		/* XXX: fix reference relation selector to support schema-qualified names */
		relName := &rfqn.RelationFQN{
			RelationName: stmt.ID,
		}

		seqs, err := mngr.ListRelationSequences(ctx, relName)
		if err != nil {
			return nil, err
		}
		for _, seq := range seqs {
			if err := mngr.DropSequence(ctx, seq, true); err != nil {
				return nil, err
			}
		}

		if err := mngr.DropReferenceRelation(ctx, relName); err != nil {
			return nil, err
		}

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("drop reference relation"),
		}

		tts.Raw = [][][]byte{
			[][]byte{
				fmt.Appendf(nil, "relation -> %s", stmt.ID),
			},
		}

		return tts, nil
	case *spqrparser.DistributionSelector:
		var krs []*kr.KeyRange
		var err error

		if stmt.ID == "*" {

			krs, err = mngr.ListAllKeyRanges(ctx)
			if err != nil {
				return nil, err
			}
		} else {

			krs, err = mngr.ListKeyRanges(ctx, stmt.ID)
			if err != nil {
				return nil, err
			}
		}

		if len(krs) != 0 && !isCascade {
			return nil, spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "cannot drop distribution %s because other objects depend on it\nHINT: Use DROP ... CASCADE to drop the dependent objects too.", stmt.ID)
		}

		for _, kr := range krs {
			err = mngr.DropKeyRange(ctx, kr.ID)
			if err != nil {
				return nil, err
			}
		}

		if stmt.ID != "*" {
			ds, err := mngr.GetDistribution(ctx, stmt.ID)
			if err != nil {
				return nil, err
			}
			if len(ds.Relations) != 0 && !isCascade {
				return nil, spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "cannot drop distribution %s because there are relations attached to it\nHINT: Use DROP ... CASCADE to detach relations automatically.", stmt.ID)
			}

			for _, rel := range ds.Relations {
				qualifiedName := &rfqn.RelationFQN{RelationName: rel.Name, SchemaName: rel.SchemaName}
				if err := mngr.AlterDistributionDetach(ctx, ds.Id, qualifiedName); err != nil {
					return nil, err
				}
			}
			if err := mngr.DropDistribution(ctx, stmt.ID); err != nil {
				return nil, err
			}

			tts := &tupleslot.TupleTableSlot{
				Desc: engine.GetVPHeader("drop distribution"),
			}

			tts.Raw = append(tts.Raw, [][]byte{
				fmt.Appendf(nil, "distribution id -> %s", stmt.ID),
			})

			return tts, nil
		}

		dss, err := mngr.ListDistributions(ctx)
		if err != nil {
			return nil, err
		}

		ret := make([]string, 0)
		for _, ds := range dss {
			if ds.Id != "default" {
				if len(ds.Relations) != 0 && !isCascade {
					return nil, spqrerror.NewWithHint(spqrerror.SPQR_INVALID_REQUEST, fmt.Sprintf("cannot drop distribution %s because there are relations attached to it", ds.Id), "HINT: Use DROP ... CASCADE to detach relations automatically.")
				}
				ret = append(ret, ds.ID())
				err = mngr.DropDistribution(ctx, ds.Id)
				if err != nil {
					return nil, err
				}
			}
		}

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("drop distribution"),
		}

		for _, id := range ret {
			tts.Raw = append(tts.Raw, [][]byte{
				fmt.Appendf(nil, "distribution id -> %s", id),
			})
		}

		return tts, nil
	case *spqrparser.ShardSelector:
		krs, err := mngr.ListAllKeyRanges(ctx)
		if err != nil {
			return nil, err
		}
		shardKrs := make([]*kr.KeyRange, 0, len(krs))
		for _, kr := range krs {
			if kr.ShardID == stmt.ID {
				shardKrs = append(shardKrs, kr)
			}
		}
		if len(shardKrs) != 0 && !isCascade {
			return nil, spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "cannot drop shard %s because other objects depend on it\nHINT: Use DROP ... CASCADE to drop the dependent objects too.", stmt.ID)
		}
		for _, kr := range shardKrs {
			if err := mngr.DropKeyRange(ctx, kr.ID); err != nil {
				return nil, err
			}
		}

		if err := mngr.DropShard(ctx, stmt.ID); err != nil {
			return nil, err
		}
		tts := &tupleslot.TupleTableSlot{}

		tts.Desc = engine.GetVPHeader("drop shard")
		tts.Raw = append(tts.Raw, [][]byte{
			fmt.Appendf(nil, "shard id -> %s", stmt.ID),
		})

		return tts, nil
	case *spqrparser.TaskGroupSelector:
		if err := mngr.RemoveMoveTaskGroup(ctx, stmt.ID); err != nil {
			return nil, err
		}

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("Drop task group"),
		}

		tts.Raw = append(tts.Raw, [][]byte{
			fmt.Appendf(nil, "task group id -> %s", stmt.ID),
		})

		return tts, nil
	case *spqrparser.SequenceSelector:
		if err := mngr.DropSequence(ctx, stmt.Name, false); err != nil {
			return nil, err
		}

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("drop sequence"),
			Raw: [][][]byte{
				{
					fmt.Appendf(nil, "sequence -> %s", stmt.Name),
				},
			},
		}

		return tts, nil
	default:
		return nil, fmt.Errorf("unknown drop statement")
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
func ProcessCreate(ctx context.Context, astmt spqrparser.Statement, mngr EntityMgr) (*tupleslot.TupleTableSlot, error) {
	switch stmt := astmt.(type) {
	case *spqrparser.ReferenceRelationDefinition:

		r := &rrelation.ReferenceRelation{
			TableName:     stmt.TableName.RelationName,
			SchemaName:    stmt.TableName.SchemaName,
			SchemaVersion: 1,
			ShardIds:      stmt.ShardIds,
		}

		if err := mngr.CreateReferenceRelation(ctx, r, rrelation.ReferenceRelationEntriesFromSQL(stmt.AutoIncrementEntries)); err != nil {
			return nil, err
		}

		/* XXX: can we already make this more SQL compliant?  */
		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("create reference table"),
			Raw: [][][]byte{
				{
					fmt.Appendf(nil, "table    -> %s", r.TableName),
				},
				{
					fmt.Appendf(nil, "shard id -> %s", strings.Join(r.ShardIds, ",")),
				},
			},
		}

		return tts, nil

	case *spqrparser.DistributionDefinition:
		if stmt.ID == "default" {
			return nil, spqrerror.New(spqrerror.SPQR_INVALID_REQUEST, "You cannot create a \"default\" distribution, \"default\" is a reserved word")
		}
		if stmt.Replicated {
			if distribution, err := createReplicatedDistribution(ctx, mngr); err != nil {
				return nil, err
			} else {
				tts := &tupleslot.TupleTableSlot{
					Desc: engine.GetVPHeader("add distribution"),
					Raw: [][][]byte{
						{
							fmt.Appendf(nil, "distribution id -> %s", distribution.ID()),
						},
					},
				}

				return tts, nil
			}
		} else {
			distribution, createError := createNonReplicatedDistribution(ctx, *stmt, mngr)
			if createError != nil {
				return nil, createError
			} else {

				tts := &tupleslot.TupleTableSlot{
					Desc: engine.GetVPHeader("add distribution"),
					Raw: [][][]byte{
						{
							fmt.Appendf(nil, "distribution id -> %s", distribution.ID()),
						},
					},
				}

				return tts, nil
			}
		}
	case *spqrparser.ShardingRuleDefinition:
		return nil, spqrerror.ShardingRulesRemoved
	case *spqrparser.KeyRangeDefinition:
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
			spqrlog.Zero.Error().Err(err).Msg("Error when adding key range")
			return nil, err
		}
		if defaultKr := DefaultKeyRangeId(ds); stmt.KeyRangeID == defaultKr {
			err := fmt.Errorf("key range %s is reserved", defaultKr)
			spqrlog.Zero.Error().Err(err).Msg("failed to create key range")
			return nil, err
		}
		req, err := kr.KeyRangeFromSQL(stmt, ds.ColTypes)
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("Error when adding key range")
			return nil, err
		}
		if err := mngr.CreateKeyRange(ctx, req); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("Error when adding key range")
			return nil, err
		}

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("add key range"),
			Raw: [][][]byte{
				{fmt.Appendf(nil, "bound -> %s", req.SendRaw()[0])},
			},
		}

		return tts, nil
	case *spqrparser.ShardDefinition:
		dataShard := topology.NewDataShard(stmt.Id, &config.Shard{
			RawHosts: stmt.Hosts,
			Type:     config.DataShard,
		})
		if err := mngr.AddDataShard(ctx, dataShard); err != nil {
			return nil, err
		}

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("add shard"),
			Raw: [][][]byte{
				{
					fmt.Appendf(nil, "shard id -> %s", dataShard.ID),
				},
			},
		}

		return tts, nil
	default:
		return nil, ErrUnknownCoordinatorCommand
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
		if stmt.Distribution == nil {
			return fmt.Errorf("failed to process 'ALTER DISTRIBUTION' statement: distribution ID is nil")
		}
		return processAlterDistribution(ctx, stmt.Element, mngr, cli, stmt.Distribution.ID)
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
func processAlterDistribution(ctx context.Context, astmt spqrparser.Statement, mngr EntityMgr, cli *clientinteractor.PSQLInteractor, dsId string) error {
	switch stmt := astmt.(type) {
	case *spqrparser.AttachRelation:

		rels := []*distributions.DistributedRelation{}

		for _, drel := range stmt.Relations {
			rels = append(rels, distributions.DistributedRelationFromSQL(drel))
		}

		if dsId == "default" {
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
			dsId = list[0].Id
		}

		selectedDistribId := dsId

		if err := mngr.AlterDistributionAttach(ctx, selectedDistribId, rels); err != nil {
			return cli.ReportError(err)
		}

		return cli.AlterDistributionAttach(ctx, selectedDistribId, rels)
	case *spqrparser.DetachRelation:
		if err := mngr.AlterDistributionDetach(ctx, dsId, stmt.RelationName); err != nil {
			return err
		}
		return cli.AlterDistributionDetach(ctx, dsId, stmt.RelationName.String())
	case *spqrparser.AlterRelation:
		if err := mngr.AlterDistributedRelation(ctx, dsId, distributions.DistributedRelationFromSQL(stmt.Relation)); err != nil {
			return err
		}
		qName := rfqn.RelationFQN{RelationName: stmt.Relation.Name, SchemaName: stmt.Relation.SchemaName}

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("alter relation"),
			Raw: [][][]byte{
				{
					fmt.Appendf(nil, "relation name   -> %s", dsId),
				},
				{
					fmt.Appendf(nil, "distribution id -> %s", qName.String()),
				},
			},
		}

		return cli.ReplyTTS(tts)
	case *spqrparser.DropDefaultShard:
		if distribution, err := mngr.GetDistribution(ctx, dsId); err != nil {
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
		if distribution, err := mngr.GetDistribution(ctx, dsId); err != nil {
			return err
		} else {
			manager := NewDefaultShardManager(distribution, mngr)
			if err := manager.CreateDefaultShard(ctx, stmt.Shard); err != nil {
				return err
			}
			return cli.MakeSimpleResponse(ctx, manager.SuccessCreateResponse(stmt.Shard))
		}
	case *spqrparser.AlterRelationV2:
		tts, err := processAlterRelation(ctx, stmt.Element, mngr, dsId, stmt.RelationName)
		if err != nil {
			return cli.ReportError(err)
		}
		return cli.ReplyTTS(tts)
	default:
		return ErrUnknownCoordinatorCommand
	}
}

// processAlterDistribution processes the given 'ALTER DISTRIBUTION ALTER RELATION' statement and performs the corresponding operation.
//
// Parameters:
// - ctx (context.Context): The context for the operation.
// - astmt (spqrparser.Statement): The alter relation statement to be processed.
// - mngr (EntityMgr): The entity manager for performing the operation.
// - cli (*clientinteractor.PSQLInteractor): The PSQL client interactor for interacting with the PSQL server.
// - dsId (string): ID of the distribution, to which the relation belongs.
// - relName (string): the name of the relation to alter.
//
// Returns:
// - error: An error if the operation fails, otherwise nil.
func processAlterRelation(ctx context.Context, astmt spqrparser.Statement, mngr EntityMgr, dsId string, relName string) (*tupleslot.TupleTableSlot, error) {
	switch stmt := astmt.(type) {
	case *spqrparser.AlterRelationSchema:
		if err := mngr.AlterDistributedRelationSchema(ctx, dsId, relName, stmt.SchemaName); err != nil {
			return nil, err
		}
		qName := rfqn.RelationFQN{RelationName: relName, SchemaName: stmt.SchemaName}

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("alter relation"),
			Raw: [][][]byte{
				{
					fmt.Appendf(nil, "distribution id -> %s", dsId),
				},

				{
					fmt.Appendf(nil, "relation name   -> %s", qName.String()),
				},
			},
		}

		return tts, nil
	case *spqrparser.AlterRelationDistributionKey:
		if err := mngr.AlterDistributedRelationDistributionKey(ctx, dsId, relName, distributions.DistributionKeyFromSQL(stmt.DistributionKey)); err != nil {
			return nil, err
		}

		/* Schema name? */
		qName := rfqn.RelationFQN{RelationName: relName}

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("alter relation"),
			Raw: [][][]byte{
				{
					fmt.Appendf(nil, "distribution id -> %s", dsId),
				},

				{
					fmt.Appendf(nil, "relation name   -> %s", qName.String()),
				},
			},
		}

		return tts, nil
	default:
		return nil, fmt.Errorf("unexpected 'ALTER RELATION' request type %T", stmt)
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
func ProcMetadataCommand(ctx context.Context, tstmt spqrparser.Statement, mgr EntityMgr, ci connmgr.ConnectionMgr, rc rclient.RouterClient, writer workloadlog.WorkloadLog, ro bool) error {
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

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("start trace messages"),
			Raw: [][][]byte{
				{
					[]byte("START TRACE MESSAGES"),
				},
			},
		}

		return cli.ReplyTTS(tts)
	case *spqrparser.StopTraceStmt:
		if writer == nil {
			return cli.ReportError(fmt.Errorf("cannot save workload from here"))
		}
		err := writer.StopLogging()
		if err != nil {
			return err
		}

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("stop trace messages"),
			Raw: [][][]byte{
				{
					[]byte("STOP TRASCE MESSAGES"),
				},
			},
		}

		return cli.ReplyTTS(tts)
	case *spqrparser.Drop:
		tts, err := processDrop(ctx, stmt.Element, stmt.CascadeDelete, mgr)
		if err != nil {
			return cli.ReportError(err)
		}
		return cli.ReplyTTS(tts)
	case *spqrparser.Create:
		tts, err := ProcessCreate(ctx, stmt.Element, mgr)
		if err != nil {
			return cli.ReportError(err)
		}
		return cli.ReplyTTS(tts)
	case *spqrparser.MoveKeyRange:
		move := &kr.MoveKeyRange{
			ShardId: stmt.DestShardID,
			Krid:    stmt.KeyRangeID,
		}

		if err := mgr.Move(ctx, move); err != nil {
			return cli.ReportError(err)
		}

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("move key range"),
			Raw: [][][]byte{
				{fmt.Appendf(nil, "move key range %v to shard %v", move.Krid, move.ShardId)},
			},
		}

		return cli.ReplyTTS(tts)
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

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("register router"),
			Raw: [][][]byte{
				{fmt.Appendf(nil, "router -> %s-%s", stmt.ID, stmt.Addr)},
			},
		}

		return cli.ReplyTTS(tts)
	case *spqrparser.UnregisterRouter:
		if err := mgr.UnregisterRouter(ctx, stmt.ID); err != nil {
			return err
		}

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("unregister router"),
			Raw:  [][][]byte{{fmt.Appendf(nil, "router id -> %s", stmt.ID)}},
		}

		return cli.ReplyTTS(tts)
	case *spqrparser.Lock:
		if _, err := mgr.LockKeyRange(ctx, stmt.KeyRangeID); err != nil {
			return err
		}

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("lock key range"),
			Raw:  [][][]byte{{fmt.Appendf(nil, "key range id -> %v", stmt.KeyRangeID)}},
		}

		return cli.ReplyTTS(tts)
	case *spqrparser.Unlock:
		if err := mgr.UnlockKeyRange(ctx, stmt.KeyRangeID); err != nil {
			return err
		}

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("unlock key range"),
			Raw:  [][][]byte{{fmt.Appendf(nil, "key range id -> %v", stmt.KeyRangeID)}},
		}

		return cli.ReplyTTS(tts)
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
	case *spqrparser.Invalidate:
		switch stmt.Target {
		case spqrparser.SchemaCacheInvalidateTarget:
			mgr.Cache().Reset()

		case spqrparser.BackendConnectionsInvalidateTarget:
			if err := ci.ForEachPool(func(p pool.Pool) error {
				return p.ForEach(func(sh shard.ShardHostCtl) error {
					sh.MarkStale() /* request backend invalidation */
					return nil
				})
			}); err != nil {
				return err
			}
		}
		return cli.CompleteMsg(0)
	case *spqrparser.StopMoveTaskGroup:
		tg, err := mgr.GetMoveTaskGroup(ctx, stmt.ID)
		if err != nil {
			return err
		}
		if tg == nil {
			_ = cli.ReplyNotice(ctx, "No move task group found to stop")
			return cli.CompleteMsg(0)
		}
		if err := mgr.StopMoveTaskGroup(ctx, stmt.ID); err != nil {
			return err
		}
		_ = cli.ReplyNotice(ctx, "Gracefully stopping task group")
		return cli.CompleteMsg(0)
	case *spqrparser.RetryMoveTaskGroup:
		taskGroup, err := mgr.GetMoveTaskGroup(ctx, stmt.ID)
		if err != nil {
			return err
		}
		if err := mgr.RetryMoveTaskGroup(ctx, stmt.ID); err != nil {
			return err
		}

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("Task group ID", "Destination shard ID", "Source key range ID", "Destination key range ID"),
		}
		if taskGroup != nil {
			tts.WriteDataRow(taskGroup.ID, taskGroup.ShardToId, taskGroup.KrIdFrom, taskGroup.KrIdTo)
		}
		return cli.ReplyTTS(tts)
	case *spqrparser.SyncReferenceTables:
		/* TODO: fix RelationSelector logic */
		if stmt.RelationSelector == "*" {
			return fmt.Errorf("SYNC REFERENCE TABLES/RELATIONS currently unsupported")
		}
		if err := mgr.SyncReferenceRelations(ctx, []*rfqn.RelationFQN{
			{RelationName: stmt.RelationSelector},
		}, stmt.ShardID); err != nil {
			return err
		}

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("relation", "shard"),
			Raw: [][][]byte{{
				fmt.Appendf(nil, "%v", stmt.RelationSelector),
				fmt.Appendf(nil, "%v", stmt.ShardID),
			}},
		}

		return cli.ReplyTTS(tts)
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
func ProcessKill(ctx context.Context, stmt *spqrparser.Kill, mngr EntityMgr, ci connmgr.ConnectionMgr, cli *clientinteractor.PSQLInteractor) error {
	spqrlog.Zero.Debug().Str("cmd", stmt.Cmd).Msg("process kill")
	switch stmt.Cmd {
	case spqrparser.ClientStr:
		ok, err := ci.Pop(stmt.Target)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("no such client %d", stmt.Target)
		}
		return cli.KillClient(stmt.Target)
	case spqrparser.BackendStr:

		ok := false

		if err := ci.ForEachPool(func(p pool.Pool) error {
			return p.ForEach(func(sh shard.ShardHostCtl) error {
				if sh.ID() == stmt.Target {
					ok = true
					sh.MarkStale() /* request backend invalidation */
				}
				return nil
			})
		}); err != nil {
			return err
		}

		if !ok {
			return fmt.Errorf("no such backend %d", stmt.Target)
		}

		return cli.KillBackend(stmt.Target)
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
func ProcessShow(ctx context.Context, stmt *spqrparser.Show, mngr EntityMgr, ci connmgr.ConnectionMgr, cli *clientinteractor.PSQLInteractor, ro bool) error {
	spqrlog.Zero.Debug().Str("cmd", stmt.Cmd).Msg("process show statement")
	switch stmt.Cmd {
	case spqrparser.BackendConnectionsStr:

		var resp []shard.ShardHostCtl
		if err := ci.ForEach(func(sh shard.ShardHostCtl) error {
			// apply filters
			resp = append(resp, sh)
			return nil
		}); err != nil {
			return err
		}

		tts, err := cli.BackendConnections(ctx, resp, stmt)
		if err != nil {
			return err
		}
		return cli.ReplyTTS(tts)
	case spqrparser.ShardsStr:
		shards, err := mngr.ListShards(ctx)
		if err != nil {
			return err
		}

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("shard"),
		}

		for _, shard := range shards {
			tts.Raw = append(tts.Raw,
				[][]byte{
					[]byte(shard.ID),
				},
			)
		}

		return cli.ReplyTTS(tts)
	case spqrparser.HostsStr:
		shards, err := mngr.ListShards(ctx)
		if err != nil {
			return err
		}

		ihc := ci.InstanceHealthChecks()

		tts := engine.HostsVirtualRelationScan(shards, ihc)
		return cli.ReplyTTS(tts)
	case spqrparser.KeyRangesStr:
		ranges, err := mngr.ListAllKeyRanges(ctx)
		if err != nil {
			return err
		}
		locksKr, err := mngr.ListKeyRangeLocks(ctx)
		if err != nil {
			return err
		}
		tts := engine.KeyRangeVirtualRelationScan(ranges, locksKr)
		return cli.ReplyTTS(tts)
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

		tts, err := cli.Clients(ctx, resp, stmt)
		if err != nil {
			return err
		}
		return cli.ReplyTTS(tts)

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
		tts := engine.InstanceVirtualRelationScan(ctx, ci)
		return cli.ReplyTTS(tts)
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

		tts, err := engine.RelationsVirtualRelationScan(dsToRels, stmt.Where)
		if err != nil {
			return err
		}
		return cli.ReplyTTS(tts)
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
		group, err := mngr.ListMoveTaskGroups(ctx)
		if err != nil {
			return err
		}
		return cli.MoveTaskGroups(ctx, group)
	case spqrparser.MoveTaskStr:
		taskList, err := mngr.ListMoveTasks(ctx)
		if err != nil {
			return err
		}
		if taskList == nil {
			return cli.CompleteMsg(0)
		}

		taskGroups := make(map[string]*tasks.MoveTaskGroup)
		for _, task := range taskList {
			group, err := mngr.GetMoveTaskGroup(ctx, task.TaskGroupID)
			if err != nil {
				return err
			}
			if group == nil {
				return fmt.Errorf("task group for task \"%s\" not found", task.ID)
			}
			taskGroups[group.ID] = group
		}

		moveTasksDsID := make(map[string]string)
		colTypes := make(map[string][]string)
		for _, task := range taskList {
			taskGroup, ok := taskGroups[task.TaskGroupID]
			if !ok {
				return fmt.Errorf("task group \"%s\" not found", task.TaskGroupID)
			}
			keyRange, err := mngr.GetKeyRange(ctx, taskGroup.KrIdFrom)
			if err != nil {
				if te, ok := err.(*spqrerror.SpqrError); ok && te.ErrorCode == spqrerror.SPQR_KEYRANGE_ERROR {
					var err2 error
					keyRange, err2 = mngr.GetKeyRange(ctx, taskGroup.KrIdTo)
					if err2 != nil {
						return fmt.Errorf("could not get source key range \"%s\": %s, not destination key range \"%s\": %s", taskGroup.KrIdFrom, err, taskGroup.KrIdTo, err2)
					}
				}
			}
			moveTasksDsID[task.ID] = keyRange.Distribution
			colTypes[keyRange.Distribution] = keyRange.ColumnTypes
		}
		return cli.MoveTasks(ctx, taskList, colTypes, moveTasksDsID)
	case spqrparser.PreparedStatementsStr:

		var resp []shard.PreparedStatementsMgrDescriptor
		if err := ci.ForEach(func(sh shard.ShardHostCtl) error {
			resp = append(resp, sh.ListPreparedStatements()...)
			return nil
		}); err != nil {
			return err
		}

		tts := engine.PreparedStatementsVirtualRelationScan(ctx, resp)
		return cli.ReplyTTS(tts)
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
	case spqrparser.Users:
		return cli.Users(ctx)
	case spqrparser.TsaCacheStr:
		cacheEntries := ci.TsaCacheEntries()
		return cli.ReplyTTS(engine.TSAVirtualRelationScan(cacheEntries))
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
