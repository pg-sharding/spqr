package main

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/spf13/cobra"
)

const (
	spqrPort = 6432
)

var (
	username string
	dbname   string
	relation string
	hostname string
	sslmode  string
)

func getConn(dbname string, retryCnt int) (*sql.DB, error) {
	pgConString := fmt.Sprintf("host=%s port=%d dbname=%s sslmode=%v user=%s", hostname, spqrPort, dbname, sslmode, username)
	fmt.Printf("using connstring %s\n", pgConString)
	for range retryCnt {
		db, err := sql.Open("postgres", pgConString)
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("error while connecting to postgresql")
			continue
		}
		return db, nil
	}
	return nil, fmt.Errorf("failed to get database connection")
}

var r = rand.New(rand.NewSource(31337))

func simple() {

	for {
		func() {
			time.Sleep(time.Duration(50+r.Intn(10)) * time.Microsecond)

			conn, err := getConn(dbname, 2)
			if err != nil {
				spqrlog.Zero.Error().Err(err).Msg("stress test FAILED")
				panic(err)
			}
			defer func(conn *sql.DB) {
				err := conn.Close()
				if err != nil {
					spqrlog.Zero.Error().Err(err).Msg("")
				}
			}(conn)

			if _, err := conn.Query(fmt.Sprintf("SELECT * FROM %s WHERE i = %d", relation, 1+r.Intn(10))); err != nil {
				spqrlog.Zero.Error().Err(err).Msg("")
				panic(err)
			}

			fmt.Printf("SELECT OK\n")
		}()
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
		wg := &sync.WaitGroup{}

		for range par {
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				simple()
			}(wg)
		}

		wg.Wait()

		return nil
	},
	SilenceUsage:  true,
	SilenceErrors: true,
}

var cmdTest = &cobra.Command{
	Use:   "test",
	Short: "SPQR stress test tool",
	CompletionOptions: cobra.CompletionOptions{
		DisableDefaultCmd: true,
	},
	RunE: func(cmd *cobra.Command, args []string) error {

		ctx, f := context.WithTimeout(context.Background(), 10*time.Second)
		defer f()

		go simple()

		<-ctx.Done()
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
	cmd.PersistentFlags().StringVarP(&username, "username", "u", "user1", "")
	cmd.PersistentFlags().StringVarP(&sslmode, "sslmode", "s", "disable", "")

	cmd.AddCommand(cmdTest)
}

func main() {
	if err := cmd.Execute(); err != nil {
		panic(err)
	}
}
