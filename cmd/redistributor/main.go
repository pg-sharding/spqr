package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/pg-sharding/spqr/pkg"
	"github.com/pg-sharding/spqr/pkg/coord"
	"github.com/pg-sharding/spqr/pkg/datatransfers"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	protos "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	chunkSize            int
	batchSize            int
	shardToID            string
	keyRangeID           string
	maxRedistributeTasks int
	qdbAddrs             []string
	coordinatorAddr      string
	dryRun               bool
	interval             time.Duration

	rootCmd = &cobra.Command{
		Use:   "spqr-redistributor",
		Short: "tools to help with key transfer",
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd: true,
		},
		Version:       pkg.SpqrVersionRevision,
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	generateTaskCmd = &cobra.Command{
		Use:   "generate-task --coordinator-addr `coordinator grpc address` --etcd-addr `etcd address`... --chunk-size size --batch-size size --key-range-id id --shard-id id --max-tasks count [--dry-run]",
		Short: "split a number of keys and redistribute them to a given shard",
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := generateTask(nil, nil); err != nil {
				if errors.Is(err, AlreadyDoneError{}) {
					spqrlog.Zero.Info().Err(err).Msg("task already done")
					return nil
				}
				return err
			}
			return nil
		},
	}

	runCmd = &cobra.Command{
		Use:   "run --coordinator-addr `coordinator grpc address` --etcd-addr `etcd address`... --chunk-size size --batch-size size --key-range-id id --shard-id id --max-tasks count [--interval duration] [--dry-run]",
		Short: "split a number of keys and redistribute them to a given shard periodically",
		RunE: func(_ *cobra.Command, _ []string) error {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()

			for range ticker.C {
				if err := generateTask(nil, nil); err != nil {
					if errors.Is(err, AlreadyDoneError{}) {
						spqrlog.Zero.Info().Err(err).Msg("task already done")
						return nil
					}
					spqrlog.Zero.Error().Err(err).Msg("error generating task, exiting")
					return err
				}
			}
			return nil
		},
	}
)

func init() {
	runCmd.Flags().DurationVar(&interval, "interval", 10*time.Second, "interval between iterations")

	generateTaskCmd.AddCommand(runCmd)
	generateTaskCmd.PersistentFlags().StringArrayVar(&qdbAddrs, "etcd-addr", []string{"localhost:2389"}, "etcd address to retrieve metadata")
	generateTaskCmd.PersistentFlags().StringVar(&coordinatorAddr, "coordinator-addr", "", "address of coordinator grpc server")
	generateTaskCmd.PersistentFlags().IntVar(&chunkSize, "chunk-size", 0, "how many keys are transferred by one redistribute task")
	generateTaskCmd.PersistentFlags().IntVar(&batchSize, "batch-size", 0, "how many keys are transferred at a time")
	generateTaskCmd.PersistentFlags().StringVar(&shardToID, "shard-id", "", "ID of the shard to transfer data to")
	generateTaskCmd.PersistentFlags().StringVar(&keyRangeID, "key-range-id", "", "ID of the key range to transfer")
	generateTaskCmd.PersistentFlags().IntVar(&maxRedistributeTasks, "max-tasks", 1, "maximum amount of redistribute tasks to run")
	generateTaskCmd.PersistentFlags().BoolVar(&dryRun, "dry-run", false, "perform a dry run")

	rootCmd.AddCommand(generateTaskCmd)
}

func main() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func generateTask(_ *cobra.Command, _ []string) error {
	if keyRangeID == "" {
		return fmt.Errorf("key range id must not be empty")
	}
	if shardToID == "" {
		return fmt.Errorf("shard-id argument must not be empty")
	}
	if chunkSize <= 0 {
		return fmt.Errorf("chunk size must be more than zero")
	}
	conn, err := grpc.NewClient(coordinatorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	db, err := qdb.NewEtcdQDB(qdbAddrs, 0)
	if err != nil {
		return fmt.Errorf("could not connect to QDB: %w", err)
	}
	c := coord.NewCoordinator(db, nil, qdb.DefaultMaxTxnSize)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	keyRange, err := c.GetKeyRange(ctx, keyRangeID)
	if err != nil {
		return err
	}
	if keyRange.ShardID == shardToID {
		return fmt.Errorf("key range \"%s\" is already on shard \"%s\", not doing anything: %w", keyRangeID, shardToID, AlreadyDoneError{})
	}
	ds, err := c.GetDistribution(ctx, keyRange.Distribution)
	if err != nil {
		return err
	}
	if len(ds.ColTypes) != 1 || (ds.ColTypes[0] != qdb.ColumnTypeInteger && ds.ColTypes[0] != qdb.ColumnTypeUinteger) {
		return fmt.Errorf("only single-column integer column type distributions are supported")
	}
	if keyRange == nil {
		return fmt.Errorf("key range \"%s\" not found", keyRangeID)
	}
	tasks, err := c.ListRedistributeTasks(ctx)
	if err != nil {
		return err
	}
	taskCount := 0
	for _, task := range tasks {
		if task.KeyRangeID == keyRangeID {
			spqrlog.Zero.Info().Str("key_range_id", keyRangeID).Msg("key range is already being redistributed, not doing anything")
			return nil
		}
		if task.ShardID == shardToID {
			taskCount++
		}
	}
	if taskCount >= maxRedistributeTasks {
		spqrlog.Zero.Info().Msg("redistribute tasks limit reached, not doing anything")
		return nil
	}
	nextBound, err := datatransfers.ResolveNextBound(ctx, keyRange, &c)
	if err != nil {
		return err
	}
	nextBoundBytes := (&kr.KeyRange{LowerBound: nextBound, ColumnTypes: ds.ColTypes}).OutFunc(0)
	nextBoundInt, _ := binary.Varint(nextBoundBytes)
	curBound, _ := binary.Varint(keyRange.OutFunc(0))
	keyRangeToRedistribute := keyRange.ID
	newBound := max(nextBoundInt-int64(chunkSize), curBound)
	if dryRun {
		spqrlog.Zero.Info().Int64("bound", newBound).Msg("redistribute key range (dry run)")
		return nil
	}

	krService := protos.NewKeyRangeServiceClient(conn)

	if nextBoundInt-int64(chunkSize) > curBound {
		buf := make([]byte, binary.MaxVarintLen64)
		binary.PutVarint(buf, newBound)
		newKeyRangeID := uuid.NewString()
		spqrlog.Zero.Info().Str("key_range_id", newKeyRangeID).Int64("bound", newBound).Msg("splitting key range")
		if _, err := krService.SplitKeyRange(ctx, &protos.SplitKeyRangeRequest{
			NewId:    newKeyRangeID,
			SourceId: keyRange.ID,
			Bound:    buf,
		}); err != nil {
			return err
		}
		keyRangeToRedistribute = newKeyRangeID
	}
	spqrlog.Zero.Info().Str("key_range_id", keyRangeToRedistribute).Msg("redistributing key range")
	_, err = krService.RedistributeKeyRange(ctx, &protos.RedistributeKeyRangeRequest{
		Krid:      keyRangeToRedistribute,
		BatchSize: int64(batchSize),
		ShardId:   shardToID,
		NoWait:    true,
		Check:     true,
		Apply:     true,
	})
	return err
}

type AlreadyDoneError struct{}

var _ error = AlreadyDoneError{}

func (e AlreadyDoneError) Error() string {
	return "key range already moved"
}
