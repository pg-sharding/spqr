package meta

import (
	"context"
	"fmt"
	"strings"

	"github.com/pg-sharding/spqr/coordinator/statistics"
	"github.com/pg-sharding/spqr/pkg"
	"github.com/pg-sharding/spqr/pkg/catalog"
	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/connmgr"
	"github.com/pg-sharding/spqr/pkg/engine"
	"github.com/pg-sharding/spqr/pkg/icp"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/rrelation"
	"github.com/pg-sharding/spqr/pkg/models/sequences"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/models/tasks"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	mtran "github.com/pg-sharding/spqr/pkg/models/transaction"
	"github.com/pg-sharding/spqr/pkg/netutil"
	"github.com/pg-sharding/spqr/pkg/pool"
	protos "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/tupleslot"
	"github.com/pg-sharding/spqr/pkg/workloadlog"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/cache"
	"github.com/pg-sharding/spqr/router/rfqn"
	sts "github.com/pg-sharding/spqr/router/statistics"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
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
	mtran.TransactionMgr

	ShareKeyRange(id string) error

	QDB() qdb.QDB
	DCStateKeeper() qdb.DCStateKeeper
	Cache() *cache.SchemaCache
}

// RouterConnector is an optional interface that EntityMgr can implement
// to provide gRPC connections to routers for querying their status.
type RouterConnector interface {
	GetRouterConn(r *topology.Router) (*grpc.ClientConn, error)
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
//
// Returns:
// - *tupleslot.TupleTableSlot: the result of the query.
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

				tts.Desc = engine.GetVPHeader("key_range_id")

				for _, k := range krs {
					tts.Raw = append(tts.Raw, [][]byte{
						[]byte(k.ID),
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

			tts.Desc = engine.GetVPHeader("key_range_id")

			tts.Raw = append(tts.Raw, [][]byte{
				[]byte(stmt.KeyRangeID),
			})

			return tts, err
		}
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
			Desc: engine.GetVPHeader("relation_name"),
		}

		tts.Raw = [][][]byte{{
			[]byte(stmt.ID),
		}}

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

			for _, idx := range ds.UniqueIndexesByID {
				if err := mngr.DropUniqueIndex(ctx, idx.ID); err != nil {
					return nil, err
				}
			}

			if stmt.ID != distributions.REPLICATED {
				for _, rel := range ds.Relations {
					qualifiedName := &rfqn.RelationFQN{RelationName: rel.Name, SchemaName: rel.SchemaName}
					if err := mngr.AlterDistributionDetach(ctx, ds.Id, qualifiedName); err != nil {
						return nil, err
					}
				}
			}

			if err := mngr.DropDistribution(ctx, stmt.ID); err != nil {
				return nil, err
			}

			tts := &tupleslot.TupleTableSlot{
				Desc: engine.GetVPHeader("distribution_id"),
			}

			tts.Raw = append(tts.Raw, [][]byte{
				[]byte(stmt.ID),
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
			Desc: engine.GetVPHeader("distribution_id"),
		}

		for _, id := range ret {
			tts.Raw = append(tts.Raw, [][]byte{
				[]byte(id),
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

		tts.Desc = engine.GetVPHeader("shard_id")
		tts.Raw = append(tts.Raw, [][]byte{
			[]byte(stmt.ID),
		})

		return tts, nil
	case *spqrparser.TaskGroupSelector:
		tg, err := mngr.GetMoveTaskGroup(ctx, stmt.ID)
		if err != nil {
			return nil, err
		}

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("task_group_id"),
		}
		if tg != nil {
			if err := mngr.RemoveMoveTaskGroup(ctx, stmt.ID); err != nil {
				return nil, err
			}
			tts.Raw = append(tts.Raw, [][]byte{
				[]byte(stmt.ID),
			})
		}
		return tts, nil
	case *spqrparser.SequenceSelector:
		rels, err := mngr.GetSequenceRelations(ctx, stmt.Name)
		if err != nil {
			return nil, err
		}
		if len(rels) > 0 && !isCascade {
			return nil, spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "cannot drop sequence %s because other objects depend on it\nHINT: Use DROP ... CASCADE to drop the dependent objects too.", stmt.Name)
		}

		for _, rel := range rels {
			if err := mngr.QDB().AlterSequenceDetachRelation(ctx, rel); err != nil {
				return nil, err
			}
		}

		if err := mngr.DropSequence(ctx, stmt.Name, false); err != nil {
			return nil, err
		}

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("name"),
			Raw: [][][]byte{
				{
					[]byte(stmt.Name),
				},
			},
		}

		return tts, nil
	case *spqrparser.UniqueIndexSelector:
		if err := mngr.DropUniqueIndex(ctx, stmt.ID); err != nil {
			return nil, err
		}
		return &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("id"),
			Raw: [][][]byte{
				{
					[]byte(stmt.ID),
				},
			},
		}, nil
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
//
// Returns:
// - distribution: created distribution.
// - error: An error if the creation encounters any issues.
func createReplicatedDistribution(ctx context.Context, mngr EntityMgr) (*distributions.Distribution, error) {
	if _, err := mngr.GetDistribution(ctx, distributions.REPLICATED); err != nil {
		distribution := &distributions.Distribution{
			Id:       distributions.REPLICATED,
			ColTypes: nil,
		}
		tranMngr := NewTranEntityManager(mngr)
		err := tranMngr.CreateDistribution(ctx, distribution)
		if err != nil {
			spqrlog.Zero.Debug().Err(err).Msg("failed to setup REPLICATED distribution (prepare phase)")
			return nil, err
		}
		if err = tranMngr.ExecNoTran(ctx); err != nil {
			spqrlog.Zero.Debug().Err(err).Msg("failed to setup REPLICATED distribution (execute phase)")
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
//
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
	tranMngr := NewTranEntityManager(mngr)
	err = tranMngr.CreateDistribution(ctx, distribution)
	if err != nil {
		return nil, err
	}
	if err = tranMngr.ExecNoTran(ctx); err != nil {
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
//
// Returns:
// - *tupleslot.TupleTableSlot: the result of the query.
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

		tableName := r.TableName
		if r.SchemaName != "" {
			tableName = r.SchemaName + "." + r.TableName
		}
		/* XXX: can we already make this more SQL compliant?  */
		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("create reference table"),
			Raw: [][][]byte{
				{
					fmt.Appendf(nil, "table    -> %s", tableName),
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
	case *spqrparser.KeyRangeDefinition:
		tranMngr := NewTranEntityManager(mngr)
		createdKr, err := createKeyRange(ctx, tranMngr, stmt)
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("Error when adding key range")
			return nil, err
		}
		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("add key range"),
			Raw: [][][]byte{
				{fmt.Appendf(nil, "bound -> %s", createdKr.SendRaw()[0])},
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
	case *spqrparser.UniqueIndexDefinition:
		ds, err := mngr.GetRelationDistribution(ctx, stmt.TableName)
		if err != nil {
			return nil, err
		}
		columns := make([]string, len(stmt.Columns))
		colTypes := make([]string, len(stmt.Columns))
		for i, typedCol := range stmt.Columns {
			columns[i] = typedCol.Column
			colTypes[i] = typedCol.Type
		}
		if err := mngr.CreateUniqueIndex(ctx, ds.ID(), &distributions.UniqueIndex{
			ID:           stmt.ID,
			RelationName: stmt.TableName,
			Columns:      columns,
			ColTypes:     colTypes,
		}); err != nil {
			return nil, err
		}

		return &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("create unique index"),
			Raw: [][][]byte{
				{
					fmt.Appendf(nil, "index ID -> %s", stmt.ID),
				},
				{
					fmt.Appendf(nil, "relation name -> %s", stmt.TableName.String()),
				},
				{
					fmt.Appendf(nil, "column names  -> %s", strings.Join(columns, ", ")),
				},
				{
					fmt.Appendf(nil, "column types -> %s", strings.Join(colTypes, ", ")),
				},
			},
		}, nil
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
//
// Returns:
// - *tupleslot.TupleTableSlot: the result of the query.
// - error: An error if the operation fails, otherwise nil.
func processAlter(ctx context.Context, astmt spqrparser.Statement, mngr EntityMgr) (*tupleslot.TupleTableSlot, error) {
	switch stmt := astmt.(type) {
	case *spqrparser.AlterDistribution:
		if stmt.Distribution == nil {
			return nil, fmt.Errorf("failed to process 'ALTER DISTRIBUTION' statement: distribution ID is nil")
		}
		return processAlterDistribution(ctx, stmt.Element, mngr, stmt.Distribution.ID)
	default:
		return nil, ErrUnknownCoordinatorCommand
	}
}

// processAlterDistribution processes the given alter distribution statement and performs the corresponding operation.
//
// Parameters:
// - ctx (context.Context): The context for the operation.
// - astmt (spqrparser.Statement): The alter distribution statement to be processed.
// - mngr (EntityMgr): The entity manager for managing entities.
// - dsId (string): The ID of the distribution to alter.
//
// Returns:
// - *tupleslot.TupleTableSlot: the result of the query.
// - error: An error if the operation fails, otherwise nil.
func processAlterDistribution(ctx context.Context,
	astmt spqrparser.Statement,
	mngr EntityMgr, dsId string) (*tupleslot.TupleTableSlot, error) {
	switch stmt := astmt.(type) {
	case *spqrparser.AttachRelation:

		rels := []*distributions.DistributedRelation{}

		for _, drel := range stmt.Relations {
			rels = append(rels, distributions.DistributedRelationFromSQL(drel))
		}

		if dsId == "default" {
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
			dsId = list[0].Id
		}

		selectedDistribId := dsId

		if err := mngr.AlterDistributionAttach(ctx, selectedDistribId, rels); err != nil {
			return nil, err
		}

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("attach table"),
		}

		for _, r := range rels {
			tts.WriteDataRow(fmt.Sprintf("relation name   -> %s", r.QualifiedName().String()))

			tts.WriteDataRow(fmt.Sprintf("distribution id -> %s", selectedDistribId))
		}

		return tts, nil
	case *spqrparser.DetachRelation:
		if err := mngr.AlterDistributionDetach(ctx, dsId, stmt.RelationName); err != nil {
			return nil, err
		}

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("detach relation"),
		}

		tts.WriteDataRow(fmt.Sprintf("relation name   -> %s", stmt.RelationName.String()))

		tts.WriteDataRow(fmt.Sprintf("distribution id -> %s", dsId))

		return tts, nil
	case *spqrparser.AlterRelation:
		if err := mngr.AlterDistributedRelation(ctx, dsId, distributions.DistributedRelationFromSQL(stmt.Relation)); err != nil {
			return nil, err
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

		return tts, nil
	case *spqrparser.DropDefaultShard:
		if distribution, err := mngr.GetDistribution(ctx, dsId); err != nil {
			return nil, err
		} else {
			manager := NewDefaultShardManager(distribution, mngr)
			if defaultShard, err := manager.DropDefaultShard(ctx); err != nil {
				return nil, err
			} else {

				tts := &tupleslot.TupleTableSlot{
					Desc: engine.GetVPHeader("drop default shard"),
				}

				tts.WriteDataRow(fmt.Sprintf("%s	-> %s", "distribution id", manager.distribution.Id))
				tts.WriteDataRow(fmt.Sprintf("%s	-> %s", "shard id", defaultShard))

				return tts, nil
			}
		}
	case *spqrparser.AlterDefaultShard:
		if distribution, err := mngr.GetDistribution(ctx, dsId); err != nil {
			return nil, err
		} else {
			manager := NewDefaultShardManager(distribution, mngr)
			if err := manager.CreateDefaultShard(ctx, stmt.Shard); err != nil {
				return nil, err
			}

			tts := &tupleslot.TupleTableSlot{
				Desc: engine.GetVPHeader("create default shard"),
			}

			tts.WriteDataRow(fmt.Sprintf("%s	-> %s", "distribution id", manager.distribution.Id))
			tts.WriteDataRow(fmt.Sprintf("%s	-> %s", "shard id", stmt.Shard))

			return tts, nil
		}
	case *spqrparser.AlterRelationV2:
		tts, err := processAlterRelation(ctx, stmt.Element, mngr, dsId, stmt.RelationName)
		if err != nil {
			return nil, err
		}
		return tts, nil
	default:
		return nil, ErrUnknownCoordinatorCommand
	}
}

// processAlterDistribution processes the given 'ALTER DISTRIBUTION ALTER RELATION' statement and performs the corresponding operation.
//
// Parameters:
// - ctx (context.Context): The context for the operation.
// - astmt (spqrparser.Statement): The alter relation statement to be processed.
// - mngr (EntityMgr): The entity manager for performing the operation.
// - dsId (string): ID of the distribution, to which the relation belongs.
// - relName (string): the name of the relation to alter.
//
// Returns:
// - *tupleslot.TupleTableSlot: the result of the query.
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
// - ci (connmgr.ConnectionMgr): The connection manager.
// - rule (*config.FrontendRule): Rule, under which the command is executed. Used to check for user's roles.
// - writer (workloadlog.WorkloadLog): The workload log writer.
//
// Returns:
// - *tupleslot.TupleTableSlot: the result of the query.
// - error: An error if the operation fails, otherwise nil.
func ProcMetadataCommand(ctx context.Context,
	tstmt spqrparser.Statement,
	mgr EntityMgr,
	ci connmgr.ConnectionMgr,
	rule *config.FrontendRule, writer workloadlog.WorkloadLog, ro bool) (*tupleslot.TupleTableSlot, error) {

	/* TODO: do not accept nil as rc here */
	spqrlog.Zero.Debug().Interface("tstmt", tstmt).Msg("proc query")

	if _, ok := tstmt.(*spqrparser.Show); ok {
		if err := catalog.GC.CheckGrants(catalog.RoleReader, rule); err != nil {
			return nil, err
		}
		return ProcessShow(ctx, tstmt.(*spqrparser.Show), mgr, ci, ro)
	}

	if ro {
		return nil, fmt.Errorf("console is in read only mode")
	}

	if err := catalog.GC.CheckGrants(catalog.RoleAdmin, rule); err != nil {
		return nil, err
	}

	switch stmt := tstmt.(type) {
	case nil:
		return nil, ErrUnknownCoordinatorCommand
	case *spqrparser.InstanceControlPoint:
		/* create control point */
		if stmt.Enable {
			err := icp.DefineICP(stmt.Name, stmt.A)
			if err != nil {
				return nil, err
			}
		} else {
			err := icp.ResetICP(stmt.Name)
			if err != nil {
				return nil, err
			}
		}
		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("control point"),
		}
		if stmt.Enable {
			tts.Raw = [][][]byte{
				{
					[]byte("ATTACH CONTROL POINT"),
				},
			}
		} else {
			tts.Raw = [][][]byte{
				{
					[]byte("DETACH CONTROL POINT"),
				},
			}
		}

		return tts, nil
	case *spqrparser.TraceStmt:
		if writer == nil {
			return nil, fmt.Errorf("cannot save workload from here")
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

		return tts, nil
	case *spqrparser.StopTraceStmt:
		if writer == nil {
			return nil, fmt.Errorf("cannot save workload from here")
		}
		err := writer.StopLogging()
		if err != nil {
			return nil, err
		}

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("stop trace messages"),
			Raw: [][][]byte{
				{
					[]byte("STOP TRASCE MESSAGES"),
				},
			},
		}

		return tts, nil
	case *spqrparser.Drop:
		return processDrop(ctx, stmt.Element, stmt.CascadeDelete, mgr)

	case *spqrparser.Create:
		return ProcessCreate(ctx, stmt.Element, mgr)
	case *spqrparser.MoveKeyRange:
		move := &kr.MoveKeyRange{
			ShardId: stmt.DestShardID,
			Krid:    stmt.KeyRangeID,
		}

		if err := mgr.Move(ctx, move); err != nil {
			return nil, err
		}

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("move key range"),
			Raw: [][][]byte{
				{fmt.Appendf(nil, "move key range %v to shard %v", move.Krid, move.ShardId)},
				{[]byte("HINT: MOVE KEY RANGE only updates metadata. Use REDISTRIBUTE KEY RANGE to also migrate data.")},
			},
		}

		return tts, nil
	case *spqrparser.RegisterRouter:
		newRouter := &topology.Router{
			ID:      stmt.ID,
			Address: stmt.Addr,
		}

		if err := mgr.RegisterRouter(ctx, newRouter); err != nil {
			return nil, fmt.Errorf("failed to register router: %s", err)
		}

		if err := mgr.SyncRouterMetadata(ctx, newRouter); err != nil {
			return nil, fmt.Errorf("failed to sync router metadata: %s", err)
		}

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("register router"),
			Raw: [][][]byte{
				{fmt.Appendf(nil, "router -> %s-%s", stmt.ID, stmt.Addr)},
			},
		}

		return tts, nil
	case *spqrparser.UnregisterRouter:
		if err := mgr.UnregisterRouter(ctx, stmt.ID); err != nil {
			return nil, err
		}

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("unregister router"),
			Raw:  [][][]byte{{fmt.Appendf(nil, "router id -> %s", stmt.ID)}},
		}

		return tts, nil
	case *spqrparser.Lock:
		if _, err := mgr.LockKeyRange(ctx, stmt.KeyRangeID); err != nil {
			return nil, err
		}

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("lock key range"),
			Raw:  [][][]byte{{fmt.Appendf(nil, "key range id -> %v", stmt.KeyRangeID)}},
		}

		return tts, nil
	case *spqrparser.Unlock:
		if err := mgr.UnlockKeyRange(ctx, stmt.KeyRangeID); err != nil {
			return nil, err
		}

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("unlock key range"),
			Raw:  [][][]byte{{fmt.Appendf(nil, "key range id -> %v", stmt.KeyRangeID)}},
		}

		return tts, nil
	case *spqrparser.Kill:
		return ProcessKill(ctx, stmt, mgr, ci)
	case *spqrparser.SplitKeyRange:
		splitKeyRange := &kr.SplitKeyRange{
			Bound:    stmt.Border.Pivots,
			SourceID: stmt.KeyRangeFromID,
			Krid:     stmt.KeyRangeID,
		}
		if err := mgr.Split(ctx, splitKeyRange); err != nil {
			return nil, err
		}

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("split key range"),
		}
		tts.WriteDataRow(fmt.Sprintf("key range id -> %v", splitKeyRange.Krid))
		tts.WriteDataRow(fmt.Sprintf("bound        -> %s", strings.ToLower(string(splitKeyRange.Bound[0]))))
		return tts, nil

	case *spqrparser.UniteKeyRange:
		uniteKeyRange := &kr.UniteKeyRange{
			BaseKeyRangeId:      stmt.KeyRangeIDL,
			AppendageKeyRangeId: stmt.KeyRangeIDR,
		}
		if err := mgr.Unite(ctx, uniteKeyRange); err != nil {
			return nil, err
		}

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("merge key ranges"),
		}
		tts.WriteDataRow(fmt.Sprintf("merge key range \"%v\" into \"%v\"", uniteKeyRange.AppendageKeyRangeId, uniteKeyRange.BaseKeyRangeId))
		return tts, nil
	case *spqrparser.Alter:
		return processAlter(ctx, stmt.Element, mgr)
	case *spqrparser.RedistributeKeyRange:
		return processRedistribute(ctx, stmt, mgr)
	case *spqrparser.Invalidate:

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("Invalidate"),
		}
		switch stmt.Target {

		case spqrparser.SchemaCacheInvalidateTarget:
			mgr.Cache().Reset()
		case spqrparser.StaleClientsInvalidateTarget:
			if err := ci.ClientPoolForeach(func(cl client.ClientInfo) error {
				if !netutil.TCP_CheckAliveness(cl.Conn()) {
					tts.WriteDataRow(fmt.Sprintf("signaled %d", cl.ID()))
					return cl.Cancel()
				}
				return nil
			}); err != nil {
				return nil, err
			}
			return tts, nil
		case spqrparser.BackendConnectionsInvalidateTarget:
			if err := ci.ForEachPool(func(p pool.Pool) error {
				return p.ForEach(func(sh shard.ShardHostCtl) error {
					sh.MarkStale() /* request backend invalidation */
					return nil
				})
			}); err != nil {
				return nil, err
			}
		}
		return tts, nil
	case *spqrparser.StopMoveTaskGroup:
		var tgs map[string]*tasks.MoveTaskGroup
		if stmt.ID == "*" {
			var err error
			tgs, err = mgr.ListMoveTaskGroups(ctx)
			if err != nil {
				return nil, err
			}

		} else {
			tg, err := mgr.GetMoveTaskGroup(ctx, stmt.ID)
			if err != nil {
				return nil, err
			}
			if tg != nil {
				tgs = map[string]*tasks.MoveTaskGroup{tg.ID: tg}
			}
		}

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("Move task group ID"),
		}

		for id := range tgs {
			if err := mgr.StopMoveTaskGroup(ctx, id); err != nil {
				return nil, err
			}
			tts.WriteDataRow(id)
		}

		return tts, nil
	case *spqrparser.RetryMoveTaskGroup:
		taskGroup, err := mgr.GetMoveTaskGroup(ctx, stmt.ID)
		if err != nil {
			return nil, err
		}
		if err := mgr.RetryMoveTaskGroup(ctx, stmt.ID); err != nil {
			return nil, err
		}

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("Task group ID", "Destination shard ID", "Source key range ID", "Destination key range ID"),
		}
		if taskGroup != nil {
			tts.WriteDataRow(taskGroup.ID, taskGroup.ShardToId, taskGroup.KrIdFrom, taskGroup.KrIdTo)
		}
		return tts, nil
	case *spqrparser.SyncReferenceTables:
		/* TODO: fix RelationSelector logic */
		if stmt.RelationSelector == "*" {
			return nil, fmt.Errorf("SYNC REFERENCE TABLES/RELATIONS currently unsupported")
		}
		if err := mgr.SyncReferenceRelations(ctx, []*rfqn.RelationFQN{
			{RelationName: stmt.RelationSelector},
		}, stmt.ShardID); err != nil {
			return nil, err
		}

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("relation", "shard"),
			Raw: [][][]byte{{
				fmt.Appendf(nil, "%v", stmt.RelationSelector),
				fmt.Appendf(nil, "%v", stmt.ShardID),
			}},
		}

		return tts, nil
	default:
		return nil, ErrUnknownCoordinatorCommand
	}
}

// TODO : unit tests

// ProcessKill processes the kill command based on the statement provided.
//
// Parameters:
// - ctx (context.Context): The context for the operation.
// - stmt (*spqrparser.Kill): The kill statement to process.
// - mngr (EntityMgr): The entity manager for managing entities.
//
// Returns:
// - *tupleslot.TupleTableSlot: the result of the query.
// - error: An error if the operation encounters any issues.
func ProcessKill(ctx context.Context,
	stmt *spqrparser.Kill,
	mngr EntityMgr,
	ci connmgr.ConnectionMgr) (*tupleslot.TupleTableSlot, error) {
	spqrlog.Zero.Debug().Str("cmd", stmt.Cmd).Msg("process kill")
	switch stmt.Cmd {
	case spqrparser.ClientStr:
		ok, err := ci.Pop(stmt.Target)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("no such client %d", stmt.Target)
		}

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("kill client"),
		}

		tts.WriteDataRow(fmt.Sprintf("client id -> %d", stmt.Target))

		return tts, nil
	case spqrparser.BackendStr:

		ok := false

		var cancelErr error

		if err := ci.ForEachPool(func(p pool.Pool) error {
			return p.ForEach(func(sh shard.ShardHostCtl) error {
				if sh.ID() == stmt.Target {
					ok = true

					/* TODO: sh.Close() for TERMINATE BACKEND */
					cancelErr = sh.Cancel()
				}
				return nil
			})
		}); err != nil {
			return nil, err
		}

		if !ok {
			return nil, fmt.Errorf("no such backend %d", stmt.Target)
		}

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("kill backend"),
		}

		tts.WriteDataRow(fmt.Sprintf("backend id -> %d", stmt.Target))

		return tts, cancelErr
	default:
		return nil, ErrUnknownCoordinatorCommand
	}
}

// TODO : unit tests

func ProcessShowExtended(ctx context.Context,
	stmt *spqrparser.Show,
	mngr EntityMgr,
	ci connmgr.ConnectionMgr,
	ro bool) (*tupleslot.TupleTableSlot, error) {
	spqrlog.Zero.Debug().Str("cmd", stmt.Cmd).Msg("process extended show statement")

	var tts *tupleslot.TupleTableSlot
	var err error

	switch stmt.Cmd {
	case spqrparser.BackendConnectionsStr:
		var resp []shard.ShardHostCtl
		if err := ci.ForEach(func(sh shard.ShardHostCtl) error {
			// apply filters
			resp = append(resp, sh)
			return nil
		}); err != nil {
			return nil, err
		}

		tts, err = engine.BackendConnectionsVirtualRelationScan(resp)
		if err != nil {
			return nil, err
		}

	case spqrparser.ShardsStr:
		shards, err := mngr.ListShards(ctx)
		if err != nil {
			return nil, err
		}

		tts = &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("shard"),
		}

		for _, shard := range shards {
			tts.Raw = append(tts.Raw,
				[][]byte{
					[]byte(shard.ID),
				},
			)
		}

	case spqrparser.HostsStr:
		shards, err := mngr.ListShards(ctx)
		if err != nil {
			return nil, err
		}

		tts = engine.HostsVirtualRelationScan(shards, ci.InstanceHealthChecks())

	case spqrparser.KeyRangesStr:
		ranges, err := mngr.ListAllKeyRanges(ctx)
		if err != nil {
			return nil, err
		}
		locksKr, err := mngr.ListKeyRangeLocks(ctx)
		if err != nil {
			return nil, err
		}

		tts = engine.KeyRangeVirtualRelationScan(ranges, locksKr)

	case spqrparser.KeyRangesExtendedStr:
		ranges, err := mngr.ListAllKeyRanges(ctx)
		if err != nil {
			return nil, err
		}

		locksKr, err := mngr.ListKeyRangeLocks(ctx)
		if err != nil {
			return nil, err
		}

		// Fetch distributions for column types.
		dists, err := mngr.ListDistributions(ctx)
		if err != nil {
			return nil, err
		}

		tts, err = engine.KeyRangeVirtualRelationScanExtended(
			ranges, locksKr, dists)
		if err != nil {
			return nil, err
		}
	case spqrparser.InstanceStr:
		tts = engine.InstanceVirtualRelationScan(ctx, ci)

	case spqrparser.ClientsStr:
		var resp []client.ClientInfo
		if err := ci.ClientPoolForeach(func(client client.ClientInfo) error {
			resp = append(resp, client)
			/* XXX: should we do this un-conditionally  or under separate setting? */
			/*  When this is executed by coordinator, c is (validly) nil*/
			if c := client.Conn(); c != nil && !netutil.TCP_CheckAliveness(c) {
				if err := client.Cancel(); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return nil, err
		}

		tts, err = engine.ClientsVirtualRelationScan(ctx, resp)
		if err != nil {
			return nil, err
		}
	case spqrparser.RelationsStr:
		dss, err := mngr.ListDistributions(ctx)
		if err != nil {
			return nil, err
		}
		dsToRels := make(map[string][]*distributions.DistributedRelation)
		for _, ds := range dss {
			if _, ok := dsToRels[ds.Id]; ok {
				return nil, spqrerror.Newf(spqrerror.SPQR_METADATA_CORRUPTION, "Duplicate values on \"%s\" distribution ID", ds.Id)
			}
			dsToRels[ds.Id] = make([]*distributions.DistributedRelation, 0)
			for _, rel := range ds.Relations {
				dsToRels[ds.Id] = append(dsToRels[ds.Id], rel)
			}
		}

		tts, err = engine.RelationsVirtualRelationScan(dsToRels)
		if err != nil {
			return nil, err
		}

		/* XXX: remove this hack */
		if stmt.Order == nil {
			stmt.Order = &spqrparser.Order{
				Col: spqrparser.ColumnRef{ColName: "table name"},
			}
		}

	case spqrparser.ReferenceRelationsStr:
		rrs, err := mngr.ListReferenceRelations(ctx)
		if err != nil {
			return nil, err
		}

		tts = engine.ReferenceRelationsScan(rrs)

		/* XXX: remove this hack */
		if stmt.Order == nil {
			stmt.Order = &spqrparser.Order{
				Col: spqrparser.ColumnRef{ColName: "table name"},
			}
		}

	case spqrparser.PreparedStatementsStr:

		var resp []shard.PreparedStatementsMgrDescriptor
		if err := ci.ForEach(func(sh shard.ShardHostCtl) error {
			resp = append(resp, sh.ListPreparedStatements()...)
			return nil
		}); err != nil {
			return nil, err
		}

		tts = engine.PreparedStatementsVirtualRelationScan(ctx, resp)

	case spqrparser.TsaCacheStr:
		if ci == nil {
			return nil, ErrUnknownCoordinatorCommand
		}

		cacheEntries := ci.TsaCacheEntries()
		tts = engine.TSAVirtualRelationScan(cacheEntries)
	case spqrparser.UniqueIndexesStr:
		idxs, err := mngr.ListUniqueIndexes(ctx)
		if err != nil {
			return nil, err
		}
		tts = engine.UniqueIndexesVirtualRelationScan(idxs)
	case spqrparser.TaskGroupStr, spqrparser.TaskGroupsStr:
		groups, err := mngr.ListMoveTaskGroups(ctx)
		if err != nil {
			return nil, err
		}
		statuses, err := mngr.GetAllTaskGroupStatuses(ctx)
		if err != nil {
			return nil, err
		}
		tts = engine.TaskGroupsVirtualRelationScan(groups, statuses)
	case spqrparser.MoveTaskStr, spqrparser.MoveTasksStr:
		taskList, err := mngr.ListMoveTasks(ctx)
		if err != nil {
			return nil, err
		}
		if taskList == nil {
			break
		}

		taskGroups := make(map[string]*tasks.MoveTaskGroup)
		for _, task := range taskList {
			group, err := mngr.GetMoveTaskGroup(ctx, task.TaskGroupID)
			if err != nil {
				return nil, err
			}
			if group == nil {
				return nil, fmt.Errorf("task group for task \"%s\" not found", task.ID)
			}
			taskGroups[group.ID] = group
		}

		moveTasksDsID := make(map[string]string)
		colTypes := make(map[string][]string)
		for _, task := range taskList {
			taskGroup, ok := taskGroups[task.TaskGroupID]
			if !ok {
				return nil, fmt.Errorf("task group \"%s\" not found", task.TaskGroupID)
			}
			keyRange, err := mngr.GetKeyRange(ctx, taskGroup.KrIdFrom)
			if err != nil {
				if te, ok := err.(*spqrerror.SpqrError); ok && te.ErrorCode == spqrerror.SPQR_KEYRANGE_ERROR {
					var err2 error
					keyRange, err2 = mngr.GetKeyRange(ctx, taskGroup.KrIdTo)
					if err2 != nil {
						return nil, fmt.Errorf("could not get source key range \"%s\": %s, not destination key range \"%s\": %s", taskGroup.KrIdFrom, err, taskGroup.KrIdTo, err2)
					}
				}
			}
			moveTasksDsID[task.ID] = keyRange.Distribution
			colTypes[keyRange.Distribution] = keyRange.ColumnTypes
		}
		tts, err = engine.MoveTasksVirtualRelationScan(taskList, colTypes, moveTasksDsID)
		if err != nil {
			return nil, err
		}
	case spqrparser.TaskGroupBoundsCacheStr:
		taskGroupId, err := engine.CheckWhereClauseColString(stmt.Where, "task_group_id")
		if err != nil {
			return nil, err
		}
		bounds, ind, err := mngr.GetMoveTaskGroupBoundsCache(ctx, taskGroupId)
		if err != nil {
			return nil, err
		}
		taskGroup, err := mngr.GetMoveTaskGroup(ctx, taskGroupId)
		if err != nil {
			return nil, err
		}
		if taskGroup == nil {
			return nil, fmt.Errorf("move task group \"%s\" not found", taskGroupId)
		}
		keyRange, err := mngr.GetKeyRange(ctx, taskGroup.KrIdFrom)
		if err != nil {
			if te, ok := err.(*spqrerror.SpqrError); ok && te.ErrorCode == spqrerror.SPQR_KEYRANGE_ERROR {
				var err2 error
				keyRange, err2 = mngr.GetKeyRange(ctx, taskGroup.KrIdTo)
				if err2 != nil {
					return nil, fmt.Errorf("could not get source key range \"%s\": %s, not destination key range \"%s\": %s", taskGroup.KrIdFrom, err, taskGroup.KrIdTo, err2)
				}
			}
		}

		tts, err = engine.TaskGroupBoundsCacheVirtualRelationScan(bounds, ind, keyRange.ColumnTypes, taskGroupId)
		if err != nil {
			return nil, err
		}
	default:
		return nil, ErrUnknownCoordinatorCommand
	}

	if stmt.Where != nil {
		tts, err = engine.FilterRows(tts, stmt.Where)
		if err != nil {
			return nil, err
		}
	}

	if stmt.GroupBy != nil {

		tts, err = engine.GroupBy(tts, stmt.GroupBy)
		if err != nil {
			return nil, err
		}
	}

	if stmt.Order != nil {
		tts.Raw, err = engine.ProcessOrderBy(tts.Raw, tts.Desc.GetColumnsMap(), stmt.Order)
		if err != nil {
			return nil, err
		}
	}

	return tts, nil
}

// RouterVersionInfo contains version information retrieved from a router.
type RouterVersionInfo struct {
	Version         string
	MetadataVersion int64
	Error           error
}

// getRouterVersions queries each router via gRPC to get its version information.
//
// Parameters:
// - ctx (context.Context): The context for the operation.
// - routers ([]*topology.Router): The list of routers to query.
// - getConnFunc (func(*topology.Router) (*grpc.ClientConn, error)): Function to get gRPC connection to a router.
//
// Returns:
// - map[string]RouterVersionInfo: A map of router addresses to version information.
func getRouterVersions(ctx context.Context, routers []*topology.Router, getConnFunc func(*topology.Router) (*grpc.ClientConn, error)) map[string]RouterVersionInfo {
	versions := make(map[string]RouterVersionInfo)

	for _, router := range routers {
		versionInfo := RouterVersionInfo{
			Version:         "N/A",
			MetadataVersion: 0,
		}

		if getConnFunc != nil {
			conn, err := getConnFunc(router)
			if err != nil {
				spqrlog.Zero.Error().Err(err).Str("router", router.Address).Msg("failed to get router connection")
				versionInfo.Error = err
			} else {
				client := protos.NewTopologyServiceClient(conn)
				resp, err := client.GetRouterStatus(ctx, &emptypb.Empty{})
				if err != nil {
					spqrlog.Zero.Error().Err(err).Str("router", router.Address).Msg("failed to query router status")
					versionInfo.Error = err
				} else {
					versionInfo.Version = resp.Version
					versionInfo.MetadataVersion = resp.MetadataVersion
				}
			}
		}

		versions[router.ID] = versionInfo
	}

	return versions
}

// ProcessShow processes the SHOW statement and returns an error if any issue occurs.
//
// Parameters:
// - ctx (context.Context): The context for the operation.
// - stmt (*spqrparser.Show): The SHOW statement to process.
// - mngr (EntityMgr): The entity manager for managing entities.
// - ci (connmgr.ConnectionMgr): The connection manager.
// - ro (bool): Whether server is read-only or not.
//
// Returns:
// - *tupleslot.TupleTableSlot: the result of the query.
// - error: An error if the operation encounters any issues.
func ProcessShow(ctx context.Context,
	stmt *spqrparser.Show,
	mngr EntityMgr,
	ci connmgr.ConnectionMgr, ro bool) (*tupleslot.TupleTableSlot, error) {
	spqrlog.Zero.Debug().Str("cmd", stmt.Cmd).Msg("process show statement")

	switch stmt.Cmd {
	case spqrparser.RoutersStr:
		resp, err := mngr.ListRouters(ctx)
		if err != nil {
			return nil, err
		}

		clientCounts := make(map[string]int)

		if err := ci.ClientPoolForeach(func(cl client.ClientInfo) error {
			routerAddr := cl.RAddr()
			clientCounts[routerAddr]++
			return nil
		}); err != nil {
			return nil, err
		}

		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("failed to get client counts")
			clientCounts = make(map[string]int)
		}

		// Try to get router versions via gRPC if the manager supports it
		var routerVersions map[string]RouterVersionInfo
		if rc, ok := mngr.(RouterConnector); ok {
			routerVersions = getRouterVersions(ctx, resp, rc.GetRouterConn)
		} else {
			// Fallback to empty map if gRPC is not available
			routerVersions = make(map[string]RouterVersionInfo)
		}

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("router", "status", "client_connections", "version", "metadata_version"),
		}

		for _, msg := range resp {
			status := string(msg.State)
			// Get version from gRPC query, or fall back to static version
			version := pkg.SpqrVersionRevision
			metadataVersion := int64(0)
			if vInfo, ok := routerVersions[msg.ID]; ok && vInfo.Error == nil {
				version = vInfo.Version
				metadataVersion = vInfo.MetadataVersion
			}

			tts.WriteDataRow(
				fmt.Sprintf("%s-%s", msg.ID, msg.Address),
				status,
				/* TODO: use id here */
				fmt.Sprintf("%d", clientCounts[msg.Address]),
				version,
				fmt.Sprintf("%d", metadataVersion),
			)
		}

		return tts, nil
	case spqrparser.PoolsStr:
		var respPools []pool.Pool

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader(
				"pool_id",
				"pool_router",
				"pool_db",
				"pool_usr",
				"pool_host",
				"used_connections",
				"idle_connections",
				"queue_residual_size"),
		}

		if err := ci.ForEachPool(func(p pool.Pool) error {
			respPools = append(respPools, p)

			statistics := p.View()
			tts.WriteDataRow(
				fmt.Sprintf("%p", p),
				statistics.RouterName,
				statistics.DB,
				statistics.Usr,
				statistics.Hostname,
				fmt.Sprintf("%d", statistics.UsedConnections),
				fmt.Sprintf("%d", statistics.IdleConnections),
				fmt.Sprintf("%d", statistics.QueueResidualSize))
			return nil
		}); err != nil {
			return nil, err
		}

		return tts, nil
	case spqrparser.VersionStr:

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("spqr_version"),
		}

		tts.WriteDataRow(pkg.SpqrVersionRevision)

		return tts, nil
	case spqrparser.CoordinatorAddrStr:
		addr, err := mngr.GetCoordinator(ctx)
		if err != nil {
			return nil, err
		}
		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("coordinator_address"),
		}
		tts.WriteDataRow(fmt.Sprintf("%v", addr))

		return tts, nil
	case spqrparser.DistributionsStr:
		dss, err := mngr.ListDistributions(ctx)
		if err != nil {
			return nil, err
		}
		var defShardIDs []string
		for _, d := range dss {
			krs, err := mngr.ListKeyRanges(ctx, d.Id)
			if err != nil {
				return nil, err
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

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("distribution_id", "column_types", "default_shard"),
		}

		for id, distribution := range dss {
			tts.WriteDataRow(distribution.Id,
				strings.Join(distribution.ColTypes, ","),
				defShardIDs[id])
		}
		return tts, nil
	case spqrparser.QuantilesStr:

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("quantile_type", "time_ms"),
		}

		quantiles := sts.GetQuantiles()
		quantilesStr := sts.GetQuantilesStr()
		spqrlog.Zero.Debug().Str("quantiles", fmt.Sprintf("%#v", quantiles)).Msg("Got quantiles")
		if len(*quantiles) != len(*quantilesStr) {
			return nil, fmt.Errorf("malformed configuration for quantilesStr")
		}

		for i := range *quantiles {
			q := (*quantiles)[i]
			qStr := (*quantilesStr)[i]
			if qStr == "" {
				qStr = fmt.Sprintf("%.2f", q)
			}
			tts.WriteDataRow(fmt.Sprintf("router_time_%s", qStr), fmt.Sprintf("%.2f", sts.GetTotalTimeQuantile(sts.StatisticsTypeRouter, q)))
			tts.WriteDataRow(fmt.Sprintf("shard_time_%s", qStr), fmt.Sprintf("%.2f", sts.GetTotalTimeQuantile(sts.StatisticsTypeShard, q)))
		}

		return tts, nil
	case spqrparser.SequencesStr:
		seqs, err := mngr.ListSequences(ctx)
		if err != nil {
			return nil, err
		}

		/* this is a bit ugly, but it's quick and dirty.
		* a better approach would be to do it like we do for all other objects.
		 */

		var sequenceVals []int64

		for _, s := range seqs {
			v, err := mngr.CurrVal(ctx, s)
			if err != nil {
				return nil, err
			}
			sequenceVals = append(sequenceVals, v)
		}

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("name", "value"),
		}

		for i, seq := range seqs {
			tts.WriteDataRow(seq, fmt.Sprintf("%d", sequenceVals[i]))
		}

		return tts, nil
	case spqrparser.IsReadOnlyStr:

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("is_read_only"),
		}
		tts.WriteDataRow(fmt.Sprintf("%v", ro))

		return tts, nil
	case spqrparser.MoveStatsStr:
		stats := statistics.GetMoveStats()

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("statistic", "time"),
		}

		for stat, time := range stats {
			tts.WriteDataRow(stat, time.String())
		}

		return tts, nil
	case spqrparser.Users:

		berules := config.RouterConfig().BackendRules

		tts := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader(
				"user",
				"dbname",
				"connection_limit",
				"connection_retries",
				"connection_timeout",
				"keep_alive",
				"tcp_user_timeout",
			),
		}

		for _, berule := range berules {
			tts.WriteDataRow(
				berule.Usr,
				berule.DB,
				fmt.Sprintf("%d", berule.ConnectionLimit),
				fmt.Sprintf("%d", berule.ConnectionRetries),
				berule.ConnectionTimeout.String(),
				berule.KeepAlive.String(),
				berule.TcpUserTimeout.String(),
			)
		}
		return tts, nil
	default:
		tts, err := ProcessShowExtended(ctx, stmt, mngr, ci, ro)
		if err != nil {
			return nil, err
		}
		return tts, nil
	}
}

// TODO : unit tests

// processRedistribute processes the REDISTRIBUTE KEY RANGE statement and returns an error if any issue occurs.
//
// Parameters:
//   - ctx (context.Context): The context for the operation.
//   - stmt (*spqrparser.Show): The REDISTRIBUTE KEY RANGE statement to process.
//   - mngr (EntityMgr): The entity manager for performing the redistribution.
//
// Returns:
// - *tupleslot.TupleTableSlot: the result of the query.
// - error: An error if the operation encounters any issues.
func processRedistribute(ctx context.Context,
	stmt *spqrparser.RedistributeKeyRange,
	mngr EntityMgr) (*tupleslot.TupleTableSlot, error) {
	spqrlog.Zero.Debug().
		Str("key range id", stmt.KeyRangeID).
		Str("destination shard id", stmt.DestShardID).
		Int("batch size", stmt.BatchSize).Msg("process redistribute")

	if stmt.BatchSize <= 0 {
		spqrlog.Zero.Debug().
			Int("batch-size-got", stmt.BatchSize).
			Int("batch-size-use", defaultBatchSize).
			Msg("redistribute: using default batch size")
		stmt.BatchSize = defaultBatchSize
	}

	if err := mngr.RedistributeKeyRange(ctx, &kr.RedistributeKeyRange{
		TaskGroupId: stmt.Id,
		KrId:        stmt.KeyRangeID,
		ShardId:     stmt.DestShardID,
		BatchSize:   stmt.BatchSize,
		Check:       stmt.Check,
		Apply:       stmt.Apply,
		NoWait:      stmt.NoWait,
	}); err != nil {
		return nil, err
	}

	tts := &tupleslot.TupleTableSlot{
		Desc: engine.GetVPHeader("redistribute key range"),
	}

	/* TODO: fix output  */
	tts.WriteDataRow(
		fmt.Sprintf("key range id         -> %s", stmt.KeyRangeID))
	tts.WriteDataRow(
		fmt.Sprintf("destination shard id -> %s", stmt.DestShardID))
	tts.WriteDataRow(
		fmt.Sprintf("batch size           -> %d", stmt.BatchSize))

	return tts, nil
}
