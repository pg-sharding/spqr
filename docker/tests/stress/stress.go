package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"

	_ "github.com/lib/pq"
)

const (
	spqrPort = 6432
)
	
var (
	username string
	dbname   string
	relation string
	hostname string
)

func getConn(ctx context.Context, dbname string, retryCnt int) (*sqlx.DB, error) {
	pgConString := fmt.Sprintf("host=%s port=%d dbname=%s sslmode=require user=%s", hostname, spqrPort, dbname, username)
	fmt.Printf("using connstring %s\n", pgConString)
	for i := 0; i < retryCnt; i++ {
		db, err := sqlx.ConnectContext(ctx, "postgres", pgConString)
		if err != nil {
			err = fmt.Errorf("error while connecting to postgresql: %w", err)
			fmt.Println(err)
			continue
		}
		return db, nil
	}
	return nil, fmt.Errorf("failed to get database connection")
}

var r = rand.New(rand.NewSource(31337))

func simple(wg *sync.WaitGroup) {
	defer wg.Done()
	ctx := context.TODO()

	for {

		time.Sleep(time.Duration(1+r.Intn(10)) * time.Second)

		conn, err := getConn(ctx, dbname, 2)
		defer conn.Close()
		if err != nil {
			panic(err)
		}

		if _, err := conn.Query(fmt.Sprintf("SELECT * FROM %s WHERE i = %d", relation, r.Intn(10))); err != nil {
			panic(err)
		}

		fmt.Println("SELECT OK\n")
	}
}

var par int

var cmd = &cobra.Command{
	Use:   "stress -p `parallel`",
	Short: "SPQR stress test tool",
	CompletionOptions: cobra.CompletionOptions{
		DisableDefaultCmd: true,
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Println("loh")

		tracelog.InfoLogger.Printf("loh2")

		wg := &sync.WaitGroup{}

		for i := 0; i < par; i++ {
			wg.Add(1)
			go simple(wg)
		}

		wg.Wait()

		return nil
	},
	SilenceUsage:  true,
	SilenceErrors: true,
}

func init() {
	cmd.PersistentFlags().IntVarP(&par, "parallel", "p", 10, "# of workers")
	cmd.PersistentFlags().StringVarP(&hostname, "host", "", "spqr_router_1_1", "")
	cmd.PersistentFlags().StringVarP(&relation, "rel", "r", "x", "")
	cmd.PersistentFlags().StringVarP(&dbname, "dbname", "d", "dbtpcc", "")
	cmd.PersistentFlags().StringVarP(&username, "usename", "u", "user1", "")
}

func main() {
	cmd.Execute()
}
