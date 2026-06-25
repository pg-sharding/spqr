package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/pg-sharding/spqr/pkg"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/coord"
	"github.com/pg-sharding/spqr/pkg/datatransfers"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/tasks"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/spf13/cobra"
)

var (
	shardDataFilePath string
	qdbAddrs          []string
	stateFilePath     string
	tableSampleSize   float64
	keyRangeId        string

	rootCmd = &cobra.Command{
		Use:   "spqr-monitor check --shard-data `path-to-shard-data config` --host `console host` --port `console port` --user `console user` --password `console password` --file `result file`",
		Short: "spqr-monitor",
		Long:  "spqr-monitor",
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd: true,
		},
		Version:       pkg.SpqrVersionRevision,
		SilenceUsage:  false,
		SilenceErrors: false,
	}

	checkCmd = &cobra.Command{
		Use:   "check",
		Short: "run check iteration",
		Run: func(_ *cobra.Command, _ []string) {
			if f, err := os.Open(stateFilePath); err == nil {
				_ = f.Close()
				_, _ = fmt.Printf("2;corruption found, check \"%s\" file\n", stateFilePath)
				return
			}
			shardData, err := config.LoadShardDataCfg(shardDataFilePath)
			if err != nil {
				_, _ = fmt.Println("0;no shard data file found, skipping...")
				return
			}
			db, err := qdb.NewEtcdQDB(qdbAddrs, 0)
			if err != nil {
				_, _ = fmt.Println("2;could not connect to QDB")
				os.Exit(1)
			}
			keyRangeByShardMap, dsMap, err := getQDBData(context.Background(), db)
			if err != nil {
				_, _ = fmt.Println("2;error getting data from QDB")
				os.Exit(1)
			}
			for id, shardConf := range shardData.ShardsData {
				keyRangeMap, ok := keyRangeByShardMap[id]
				if !ok {
					continue
				}
				vals, relName, err := checkShard(context.Background(), shardConf, keyRangeMap, dsMap, tableSampleSize)
				if err != nil {
					_, _ = fmt.Printf("2;error running check on shard \"%s\": %s\n", id, err)
					os.Exit(1)
				}
				if vals != nil {
					f, err := os.OpenFile(stateFilePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
					if err != nil {
						_, _ = fmt.Printf("2;failed to open state file: %s\n", err)
						os.Exit(1)
					}
					if _, err := fmt.Fprintf(f, "Corruption found: row %v, rel \"%s\" shard \"%s\"\n", vals, relName, id); err != nil {
						_, _ = fmt.Printf("2;failed to write into state file: %s", err)
						os.Exit(1)
					}
					_, _ = fmt.Printf("2;corruption found, check \"%s\" file\n", stateFilePath)
					return
				}
			}
			fmt.Println("0;OK")
		},
	}
	recoverKeyRangesCmd = &cobra.Command{
		Use:   "recover",
		Short: "run locked key range recovery iteration",
		RunE: func(cmd *cobra.Command, args []string) error {
			shardData, err := config.LoadShardDataCfg(shardDataFilePath)
			if err != nil {
				_, _ = fmt.Println("no shard data file found, skipping...")
				return nil
			}
			db, err := qdb.NewEtcdQDB(qdbAddrs, 0)
			if err != nil {
				return fmt.Errorf("could not connect to QDB: %w", err)
			}
			ctx := context.TODO()
			failedTaskGroups, err := getFailedTaskGroups(ctx, db)
			if err != nil {
				return fmt.Errorf("could not get failed task groups: %w", err)
			}
			lockedDbKeyRanges, err := getTaskGroupsWithLockedKeyRanges(ctx, db, failedTaskGroups)
			if err != nil {
				return fmt.Errorf("could not get locked key ranges: %w", err)
			}
			dsMap := map[string]*distributions.Distribution{}
			lockedKeyRanges := map[string]*kr.KeyRange{}
			for id, keyRangeDb := range lockedDbKeyRanges {
				ds, ok := dsMap[keyRangeDb.DistributionId]
				if !ok {
					ds, err := db.GetDistribution(ctx, keyRangeDb.DistributionId)
					if err != nil {
						return fmt.Errorf("could not get distribution: %w", err)
					}
					dsMap[ds.ID] = distributions.DistributionFromDB(ds)
				}
				keyRange, err := kr.KeyRangeFromDB(keyRangeDb, ds.ColTypes)
				if err != nil {
					return fmt.Errorf("error converting key range: %w", err)
				}
				lockedKeyRanges[id] = keyRange
			}
			for taskGroupId, keyRange := range lockedKeyRanges {
				if err := processKeyRange(ctx, db, taskGroupId, keyRange, shardData, dsMap); err != nil {
					return err
				}
			}
			return nil
		},
	}
	verifyKeyRangeCmd = &cobra.Command{
		Use:   "verify",
		Short: "verify key range for unlock",
		RunE: func(cmd *cobra.Command, args []string) error {
			shardData, err := config.LoadShardDataCfg(shardDataFilePath)
			if err != nil {
				_, _ = fmt.Println("no shard data file found, skipping...")
				return nil
			}
			db, err := qdb.NewEtcdQDB(qdbAddrs, 0)
			if err != nil {
				return fmt.Errorf("could not connect to QDB: %w", err)
			}
			ctx := context.Background()
			c := coord.NewCoordinator(db, nil, qdb.DefaultMaxTxnSize)
			keyRange, err := c.GetKeyRange(ctx, keyRangeId)
			if err != nil {
				return err
			}
			ds, err := c.GetDistribution(ctx, keyRange.Distribution)
			if err != nil {
				return err
			}
			moveTasks, err := c.ListMoveTasks(ctx)
			if err != nil {
				return err
			}

			var shardToConn *config.ShardConnect
			var shardFromConn *config.ShardConnect
			for _, moveTask := range moveTasks {
				if moveTask.KridTemp == keyRangeId {
					taskGroup, err := c.GetMoveTaskGroup(ctx, moveTask.TaskGroupID)
					if err != nil {
						return err
					}
					var ok bool
					shardFromConn, ok = shardData.ShardsData[taskGroup.KridFrom]
					if !ok {
						return fmt.Errorf("source key range \"%s\" not found in shard_data config", taskGroup.KridFrom)
					}
					shardToConn, ok = shardData.ShardsData[taskGroup.KridTo]
					if !ok {
						return fmt.Errorf("destination key range \"%s\" not found in shard_data config", taskGroup.KridFrom)
					}
				}
			}

			if shardToConn == nil {
				return fmt.Errorf("key range \"%s\" does not belong to any move task", keyRangeId)
			}
			return checkUnlockKeyRange(ctx, db, keyRange, ds, shardToConn, shardFromConn)
		},
	}
)

func init() {
	checkCmd.PersistentFlags().StringVarP(&shardDataFilePath, "shard-data", "c", "/etc/spqr/shard-data.yaml", "path to shard data config")
	checkCmd.PersistentFlags().StringArrayVar(&qdbAddrs, "etcd-addr", []string{"localhost:2389"}, "etcd address to retrieve metadata")
	checkCmd.PersistentFlags().StringVar(&stateFilePath, "file", "", "result file path")
	checkCmd.PersistentFlags().Float64Var(&tableSampleSize, "tablesample-size", 0.01, "query table sample size in percents")

	recoverKeyRangesCmd.PersistentFlags().StringVarP(&shardDataFilePath, "shard-data", "c", "/etc/spqr/shard-data.yaml", "path to shard data config")
	recoverKeyRangesCmd.PersistentFlags().StringArrayVar(&qdbAddrs, "etcd-addr", []string{"localhost:2389"}, "etcd address to retrieve metadata")

	verifyKeyRangeCmd.PersistentFlags().StringVarP(&shardDataFilePath, "shard-data", "c", "/etc/spqr/shard-data.yaml", "path to shard data config")
	verifyKeyRangeCmd.PersistentFlags().StringArrayVar(&qdbAddrs, "etcd-addr", []string{"localhost:2389"}, "etcd address to retrieve metadata")
	verifyKeyRangeCmd.PersistentFlags().StringVarP(&keyRangeId, "key-range", "k", "", "ID of the key range to check")

	rootCmd.AddCommand(checkCmd)
	rootCmd.AddCommand(recoverKeyRangesCmd)
	rootCmd.AddCommand(verifyKeyRangeCmd)
}

func main() {
	_ = rootCmd.Execute()
}

type keyRangeExt struct {
	*kr.KeyRange
	UpperBound kr.KeyRangeBound
}

func getQDBData(ctx context.Context, db *qdb.EtcdQDB) (keyRangesMap map[string]map[string][]*keyRangeExt, distributionsMap map[string]*distributions.Distribution, err error) {
	keyRangesMap = make(map[string]map[string][]*keyRangeExt)
	distributionsMap = make(map[string]*distributions.Distribution)
	dss, err := db.ListDistributions(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("could not list distributions: %w", err)
	}
	for _, ds := range dss {
		distributionsMap[ds.ID] = distributions.DistributionFromDB(ds)
		krs, err := db.ListKeyRanges(ctx, ds.ID)
		if err != nil {
			return nil, nil, fmt.Errorf("could not list key ranges: %w", err)
		}
		krsInternal := make([]*keyRangeExt, len(krs))
		for i, krDb := range krs {
			keyRange, err := kr.KeyRangeFromDB(krDb, ds.ColTypes)
			if err != nil {
				return nil, nil, err
			}
			krsInternal[i] = &keyRangeExt{
				KeyRange: keyRange,
			}
		}
		slices.SortFunc(krsInternal, func(l *keyRangeExt, r *keyRangeExt) int {
			if kr.CmpRangesLess(l.LowerBound, r.LowerBound, ds.ColTypes) {
				return -1
			}
			if kr.CmpRangesEqual(l.LowerBound, r.LowerBound, ds.ColTypes) {
				return 0
			}
			return 1
		})
		for i := range len(krsInternal) - 1 {
			krsInternal[i].UpperBound = krsInternal[i+1].UpperBound
		}
		for _, keyRange := range krsInternal {
			if _, ok := keyRangesMap[keyRange.ShardID]; !ok {
				keyRangesMap[keyRange.ShardID] = map[string][]*keyRangeExt{}
			}
			if _, ok := keyRangesMap[keyRange.ShardID][ds.ID]; !ok {
				keyRangesMap[keyRange.ShardID][ds.ID] = make([]*keyRangeExt, 0)
			}
			keyRangesMap[keyRange.ShardID][ds.ID] = append(keyRangesMap[keyRange.ShardID][ds.ID], keyRange)
		}
	}

	return keyRangesMap, distributionsMap, nil
}

func checkShard(ctx context.Context, shardConn *config.ShardConnect, keyRangesMap map[string][]*keyRangeExt, distributionsMap map[string]*distributions.Distribution, tableSampleSize float64) ([]any, string, error) {
	connConfig, err := pgx.ParseConfig(config.AddTSA(shardConn.GetCombinedConnString(), "prefer-standby"))
	if err != nil {
		return nil, "", err
	}
	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return nil, "", err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	defer func() { _ = conn.Close(ctx) }()
	for dsId, krs := range keyRangesMap {
		ds, ok := distributionsMap[dsId]
		if !ok {
			return nil, "", fmt.Errorf("distribution \"%s\" not found in distributions map", dsId)
		}
		if len(ds.FQNRelations)+len(ds.Relations) == 0 {
			continue
		}
		rels := make([]*distributions.DistributedRelation, 0, len(ds.FQNRelations)+len(ds.Relations))
		for _, rel := range ds.Relations {
			rels = append(rels, rel)
		}
		for _, rel := range ds.FQNRelations {
			rels = append(rels, rel)
		}
		for _, rel := range rels {
			tableExists, err := datatransfers.CheckTableExists(ctx, tx, rel.Relation)
			if err != nil {
				return nil, "", fmt.Errorf("failed to check if relation \"%s\" exists: %w", rel.Relation.String(), err)
			}
			if !tableExists {
				continue
			}
			krQueries := make([]string, 0, len(krs))
			for _, keyRange := range krs {
				cond, err := kr.GetKRCondition(rel, keyRange.KeyRange, keyRange.UpperBound, "")
				if err != nil {
					return nil, "", err
				}
				krQueries = append(krQueries, "("+cond+")")
			}
			rows, err := tx.Query(ctx, fmt.Sprintf("SELECT * FROM %s TABLESAMPLE SYSTEM(%f) WHERE NOT (%s) LIMIT 1", rel.Relation.String(), tableSampleSize, strings.Join(krQueries, " OR ")))
			if err != nil {
				return nil, "", err
			}
			defer rows.Close()
			if rows.Next() {
				vals, err := rows.Values()
				if err != nil {
					return nil, "", err
				}
				return vals, rel.Relation.String(), nil
			}
		}
	}
	_ = tx.Commit(ctx)
	return nil, "", nil
}

func getFailedTaskGroups(ctx context.Context, db *qdb.EtcdQDB) (map[string]*qdb.MoveTaskGroup, error) {
	taskGroups, err := db.ListTaskGroups(ctx)
	if err != nil {
		return nil, err
	}
	for id, _ := range taskGroups {
		status, err := db.GetTaskGroupStatus(ctx, id)
		if err != nil {
			return nil, err
		}
		if status.State != string(tasks.TaskGroupError) {
			delete(taskGroups, id)
		}
	}
	return taskGroups, nil
}

func getTaskGroupsWithLockedKeyRanges(ctx context.Context, db *qdb.EtcdQDB, taskGroups map[string]*qdb.MoveTaskGroup) (map[string]*qdb.KeyRange, error) {
	res := make(map[string]*qdb.KeyRange)
	for id := range taskGroups {
		task, err := db.GetMoveTaskByGroup(ctx, id)
		if err != nil {
			return nil, err
		}
		if task == nil {
			delete(taskGroups, id)
			continue
		}
		keyRange, err := db.GetKeyRange(ctx, task.KrIdTemp)
		if err != nil {
			return nil, err
		}
		if keyRange.Locked {
			res[id] = keyRange
		}
	}
	return res, nil
}

func checkUnlockKeyRange(ctx context.Context, db *qdb.EtcdQDB, keyRange *kr.KeyRange, ds *distributions.Distribution, shardToConnCfg, shardFromConnCfg *config.ShardConnect) error {
	mngr := coord.NewCoordinator(db, nil, qdb.DefaultMaxTxnSize)
	nextBound, err := datatransfers.ResolveNextBound(ctx, keyRange, &mngr)
	if err != nil {
		return err
	}
	fromConn, err := datatransfers.GetMasterConnection(ctx, shardFromConnCfg, "")
	if err != nil {
		return err
	}
	fromCounts, err := getEntriesCountByRelation(ctx, keyRange, nextBound, fromConn, ds)
	if err != nil {
		return err
	}
	toConn, err := datatransfers.GetMasterConnection(ctx, shardToConnCfg, "")
	if err != nil {
		return err
	}
	toCounts, err := getEntriesCountByRelation(ctx, keyRange, nextBound, toConn, ds)
	if err != nil {
		return err
	}
	for rel, fromCount := range fromCounts {
		toCount, ok := toCounts[rel]
		if !ok {
			return fmt.Errorf("malformed map of relations to entries count")
		}
		if toCount != 0 && fromCount != toCount {
			return fmt.Errorf("cannot unlock key range: in relation \"%s\" %d entries on source shard, %d entries on destination shard", rel, fromCount, toCount)
		}
	}
	// key range safe to unlock (TODO: possibly delete from dest shard?)
	return nil
}

func getEntriesCountByRelation(ctx context.Context, keyRange *kr.KeyRange, nextBound kr.KeyRangeBound, conn *pgx.Conn, ds *distributions.Distribution) (map[string]int, error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	relToCount := map[string]int{}
	for _, rel := range ds.ListRelations() {
		tableExists, err := datatransfers.CheckTableExists(ctx, tx, rel.Relation)
		if err != nil {
			return nil, err
		}
		relFullName := rel.QualifiedName().String()
		if !tableExists {
			relToCount[relFullName] = 0
			continue
		}
		krCondition, err := kr.GetKRCondition(rel, keyRange, nextBound, "")
		if err != nil {
			return nil, err
		}
		count, err := datatransfers.GetEntriesCount(ctx, tx, relFullName, krCondition)
		if err != nil {
			return nil, err
		}
		relToCount[relFullName] = count
	}
	_ = tx.Commit(ctx)
	return relToCount, nil
}

func processKeyRange(ctx context.Context, db *qdb.EtcdQDB, taskGroupId string, keyRange *kr.KeyRange, shardData *config.DatatransferConnections, dsMap map[string]*distributions.Distribution) error {
	// 1. Lock task group
	// 2. Check & unlock key range
	// 3. Delete/move task group & respective redistribute task
	// TODO: transactional unlock & delete
	if err := db.TryTaskGroupLock(ctx, taskGroupId, "spqr-monitor recover"); err != nil {
		log.Printf("failed to lock task group \"%s\", skipping...\n", taskGroupId)
		return nil
	}
	taskGroup, err := db.GetMoveTaskGroup(ctx, taskGroupId)
	if err != nil {
		return err
	}
	fromConnCfg, ok := shardData.ShardsData[taskGroup.KrIdFrom]
	if !ok {
		return fmt.Errorf("source key range \"%s\" not found in shard_data config", taskGroup.KrIdFrom)
	}
	toConnCfg, ok := shardData.ShardsData[taskGroup.KrIdTo]
	if !ok {
		return fmt.Errorf("destination key range \"%s\" not found in shard_data config", taskGroup.KrIdFrom)
	}
	ds, ok := dsMap[keyRange.Distribution]
	if !ok {
		return fmt.Errorf("distribution \"%s\" not found in map", keyRange.Distribution)
	}
	if err := checkUnlockKeyRange(ctx, db, keyRange, ds, toConnCfg, fromConnCfg); err != nil {
		log.Printf("key range not safe to unlock: %s", err)
		_ = db.DropTaskGroupLock(ctx, taskGroupId)
		return nil
	}
	if err := db.UnlockKeyRange(ctx, keyRange.ID); err != nil {
		return err
	}
	var moveOp *qdb.MoveKeyRange
	// TODO: lock key range for moves somehow?? Or check for auto-recovery to be disabled
	ls, err := db.ListKeyRangeMoves(ctx)
	if err != nil {
		return err
	}

	for _, krm := range ls {
		if krm.KeyRangeID == keyRange.ID {
			moveOp = krm
		}
	}
	if moveOp != nil {
		if err := db.DeleteKeyRangeMove(ctx, moveOp.MoveId); err != nil {
			return err
		}
	}

	tx, err := db.GetTransferTx(ctx, keyRange.ID)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("error getting data transfer transaction from qdb")
	}
	if tx != nil {
		if err := db.RemoveTransferTx(ctx, keyRange.ID); err != nil {
			return err
		}
	}
	status, err := db.GetTaskGroupStatus(ctx, taskGroupId)
	if err != nil {
		return err
	}

	log.Printf("deleting task group \"%s\". source key range: \"%s\", dest key range: \"%s\", state: \"%s\", error msg: \"%s\"", taskGroupId, taskGroup.KrIdFrom, taskGroup.KrIdTo, status.State, status.Message)
	if err := db.DropMoveTaskGroup(ctx, taskGroupId); err != nil {
		return err
	}
	return nil
}
