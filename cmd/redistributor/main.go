package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"os"

	"github.com/gofrs/uuid"
	"github.com/pg-sharding/spqr/pkg"
	"github.com/pg-sharding/spqr/pkg/coord"
	"github.com/pg-sharding/spqr/pkg/datatransfers"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	protos "github.com/pg-sharding/spqr/pkg/protos"
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

	rootCmd = &cobra.Command{
		Use:   "spqr-redistributor command args...",
		Short: "tools to help with key transfer",
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd: true,
		},
		Version:       pkg.SpqrVersionRevision,
		SilenceUsage:  false,
		SilenceErrors: false,
	}

	generateTaskCmd = &cobra.Command{
		Use:   "TODO",
		Short: "split a number of keys and redistribute them to a given shard",
		RunE: func(cmd *cobra.Command, args []string) error {
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
			ctx := context.Background()
			keyRange, err := c.GetKeyRange(ctx, keyRangeID)
			if err != nil {
				return err
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
				if task.ShardID == shardToID {
					taskCount++
				}
			}
			if taskCount >= maxRedistributeTasks {
				log.Println("redistribute tasks limit reached, not doing anything")
				return nil
			}
			nextBound, err := datatransfers.ResolveNextBound(ctx, keyRange, &c)
			nextBoundBytes := (&kr.KeyRange{LowerBound: nextBound}).OutFunc(0)
			nextBoundInt, _ := binary.Varint(nextBoundBytes)
			curBound, _ := binary.Varint(keyRange.OutFunc(0))
			keyRangeToRedistribute := keyRange.ID
			newBound := max(nextBoundInt-int64(chunkSize), curBound)
			if dryRun {
				log.Printf("redistribute key range with bound %d\n", newBound)
				return nil
			}
			if nextBoundInt-int64(chunkSize) > curBound {
				buf := make([]byte, binary.MaxVarintLen64)
				binary.PutVarint(buf, newBound)
				newKeyRangeID, err := uuid.NewV4()
				if err != nil {
					return err
				}
				log.Printf("splitting key range \"%s\" by %d\n", newKeyRangeID.String(), newBound)
				if err := c.Split(ctx, &kr.SplitKeyRange{
					Bound:      [][]byte{buf},
					SourceID:   keyRange.ID,
					KeyRangeID: newKeyRangeID.String(),
				}); err != nil {
					return err
				}
				keyRangeToRedistribute = newKeyRangeID.String()
			}
			log.Printf("redistributing key range \"%s\"\n", keyRangeToRedistribute)
			krService := protos.NewKeyRangeServiceClient(conn)
			_, err = krService.RedistributeKeyRange(ctx, &protos.RedistributeKeyRangeRequest{
				Krid:      keyRangeToRedistribute,
				BatchSize: int64(batchSize),
				ShardId:   shardToID,
				NoWait:    true,
			})
			return err
		},
	}
)

func init() {
	generateTaskCmd.Flags().StringArrayVar(&qdbAddrs, "etcd-addr", []string{"localhost:2389"}, "etcd address to retrieve metadata")
	generateTaskCmd.Flags().StringVar(&coordinatorAddr, "coordinator-addr", "", "address of coordinator grpc server")
	generateTaskCmd.Flags().IntVar(&chunkSize, "chunk-size", 0, "how many keys are transferred by one redistribute task")
	generateTaskCmd.Flags().IntVar(&batchSize, "batch-size", 0, "how many keys are transferred at a time")
	generateTaskCmd.Flags().StringVar(&shardToID, "shard-id", "", "ID of the shard to transfer data to")
	generateTaskCmd.Flags().StringVar(&keyRangeID, "key-range-id", "", "ID of the key range to transfer")
	generateTaskCmd.Flags().IntVar(&maxRedistributeTasks, "max-tasks", 1, "maximum amount of redistribute tasks to run")
	generateTaskCmd.Flags().BoolVar(&dryRun, "dry-run", false, "perform a dry run")
}

func main() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}
