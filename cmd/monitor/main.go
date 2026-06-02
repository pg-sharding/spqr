package main

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/pg-sharding/spqr/pkg"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/spf13/cobra"
)

var (
	shardDataFilePath string
	qdbAddrs          []string
	stateFilePath     string
	tableSampleSize   float64

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
			shardData, err := config.LoadShardDataCfg(shardDataFilePath)
			if err != nil {
				_, _ = fmt.Println("no shard data file found, skipping...")
				return
			}
			db, err := qdb.NewEtcdQDB(qdbAddrs, 0)
			if err != nil {
				_, _ = fmt.Fprintln(os.Stderr, "could not connect to QDB")
				os.Exit(1)
			}
			keyRangeByShardMap, dsMap, err := getQDBData(context.Background(), db)
			if err != nil {
				_, _ = fmt.Fprintln(os.Stderr, "error getting data from QDB")
				os.Exit(1)
			}
			for id, shardConf := range shardData.ShardsData {
				keyRangeMap, ok := keyRangeByShardMap[id]
				if !ok {
					continue
				}
				vals, relName, err := checkShard(context.Background(), shardConf, keyRangeMap, dsMap, tableSampleSize)
				if err != nil {
					_, _ = fmt.Fprintf(os.Stderr, "error running check on shard \"%s\": %s\n", id, err)
					os.Exit(1)
				}
				if vals != nil {
					f, err := os.OpenFile(stateFilePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
					if err != nil {
						_, _ = fmt.Fprintf(os.Stderr, "failed to open state file: %s\n", err)
						os.Exit(1)
					}
					if _, err := fmt.Fprintf(f, "Corruption found: row %v, rel \"%s\" shard \"%s\"\n", vals, relName, id); err != nil {
						_, _ = fmt.Fprintf(os.Stderr, "failed to write into state file: %s", err)
						os.Exit(1)
					}
					_, _ = fmt.Fprintf(os.Stderr, "corruption found, check \"%s\" file\n", stateFilePath)
					os.Exit(2)
				}
			}
		},
	}
)

func init() {
	rootCmd.PersistentFlags().StringVarP(&shardDataFilePath, "shard-data", "c", "/etc/spqr/shard-data.yaml", "path to shard data config")
	rootCmd.PersistentFlags().StringArrayVar(&qdbAddrs, "etcd-addr", []string{"localhost:2389"}, "etcd address to retrieve metadata")
	rootCmd.PersistentFlags().StringVar(&stateFilePath, "file", "", "result file path")
	rootCmd.PersistentFlags().Float64Var(&tableSampleSize, "tablesample-size", 0.01, "query table sample size in percents")

	rootCmd.AddCommand(checkCmd)
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
