//go:build simple || xproto || all

package prep_stmt_test

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const upsertBenchTimeout = 10 * time.Second

// BenchmarkDistributedUpsert contends on one row per shard. Each operation is
// an autocommit statement, so unrelated transaction lock ordering cannot mask
// problems with dispatching one distributed INSERT. Run with -benchtime=10000x
// (or longer) to exercise concurrent clients beyond benchmark calibration.
func BenchmarkDistributedUpsert(b *testing.B) {
	if isPurePGTesting() {
		b.Skip("requires the four-shard xproto environment")
	}

	admin := upsertBenchConnect(b, true)
	control := upsertBenchConnect(b, false)
	table := fmt.Sprintf("xproto_upsert_%x", time.Now().UnixNano())
	distribution := table + "_ds"

	upsertBenchExec(b, admin, "CREATE DISTRIBUTION "+distribution+" COLUMN TYPES integer")
	b.Cleanup(func() {
		upsertBenchExec(b, admin, "DROP DISTRIBUTION "+distribution+" CASCADE")
	})
	// Create upper ranges first: a new range extends to the next existing bound.
	bounds := []int{0, 11, 1000, 2000}
	for i := len(bounds) - 1; i >= 0; i-- {
		upsertBenchExec(b, admin, fmt.Sprintf(
			"CREATE KEY RANGE %s_kr%d FROM %d ROUTE TO sh%d FOR DISTRIBUTION %s",
			table, i, bounds[i], i+1, distribution))
	}
	upsertBenchExec(b, admin, "CREATE RELATION "+table+" (id) IN "+distribution)
	upsertBenchExec(b, control, "CREATE TABLE "+table+" (id integer PRIMARY KEY, updates bigint NOT NULL)")
	b.Cleanup(func() {
		upsertBenchExec(b, control, "DROP TABLE "+table)
	})

	for _, protocol := range []string{"Simple", "Extended"} {
		b.Run(protocol, func(b *testing.B) {
			for _, linearize := range []bool{false, true} {
				name := "Auto"
				if linearize {
					name = "Forced"
				}
				b.Run(name, func(b *testing.B) {
					benchmarkDistributedUpsert(b, control, table, protocol == "Extended", linearize)
				})
			}
		})
	}
}

func benchmarkDistributedUpsert(b *testing.B, control *pgx.Conn, table string, extended, linearize bool) {
	b.StopTimer()
	upsertBenchExec(b, control, "TRUNCATE "+table)
	upsertBenchExec(b, control, "INSERT INTO "+table+" VALUES (1, 0), (12, 0), (1001, 0), (2001, 0)")

	// Rotate the key order across clients and iterations. INSERT SELECT reads
	// each shard's local key and needs no multi-VALUES rewriting in either
	// protocol. One key per shard avoids intra-shard row-order deadlocks.
	keys := []int{1, 12, 1001, 2001}
	queries := make([]string, len(keys))
	parameters := make([][][]byte, len(keys))
	for offset := range keys {
		parameters[offset] = make([][]byte, len(keys))
		for i := range keys {
			parameters[offset][i] = []byte(strconv.Itoa(keys[(i+offset)%len(keys)]))
		}
		queries[offset] = fmt.Sprintf(
			"INSERT INTO %s (id, updates) SELECT id, 1 FROM %s WHERE id IN (%s, %s, %s, %s) "+
				"ON CONFLICT (id) DO UPDATE SET updates = %s.updates + 1",
			table, table, parameters[offset][0], parameters[offset][1], parameters[offset][2], parameters[offset][3], table)
	}
	parameterized := fmt.Sprintf(
		"INSERT INTO %s (id, updates) SELECT id, 1 FROM %s WHERE id IN ($1, $2, $3, $4) "+
			"ON CONFLICT (id) DO UPDATE SET updates = %s.updates + 1", table, table, table)

	clients := make([]*pgx.Conn, min(8, b.N))
	for i := range clients {
		clients[i] = upsertBenchConnect(b, false)
		upsertBenchExec(b, clients[i], fmt.Sprintf("SET __spqr__.linearize_dispatch TO %t", linearize))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	start := make(chan struct{})
	errors := make(chan error, 1)
	var next atomic.Int64
	var workers sync.WaitGroup
	for worker, conn := range clients {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for ctx.Err() == nil {
				iteration := next.Add(1) - 1
				if iteration >= int64(b.N) {
					return
				}
				offset := (int(iteration) + worker) % len(keys)
				queryCtx, queryCancel := context.WithTimeout(ctx, upsertBenchTimeout)
				var tag pgconn.CommandTag
				var err error
				if extended {
					// ExecParams always uses Parse/Bind/Execute/Sync. pgx.Exec
					// with no arguments would silently use the simple protocol.
					tag, err = conn.PgConn().ExecParams(queryCtx, parameterized,
						parameters[offset], nil, nil, nil).Close()
				} else {
					tag, err = conn.Exec(queryCtx, queries[offset])
				}
				queryCancel()
				if err == nil && (!tag.Insert() || conn.PgConn().TxStatus() != 'I') {
					err = fmt.Errorf("unexpected completion: tag %q, transaction status %c", tag, conn.PgConn().TxStatus())
				}
				if err != nil {
					select {
					case errors <- fmt.Errorf("client %d, operation %d, key rotation %d: %w", worker, iteration, offset, err):
					default:
					}
					cancel()
					return
				}
			}
		}()
	}
	b.ResetTimer()
	b.StartTimer()
	close(start)
	workers.Wait()
	b.StopTimer()
	select {
	case err := <-errors:
		b.Fatal(err)
	default:
	}
	b.ReportMetric(float64(len(clients)), "clients")
	b.ReportMetric(float64(len(keys)), "rows/op")

	checkCtx, checkCancel := context.WithTimeout(context.Background(), upsertBenchTimeout)
	defer checkCancel()
	rows, err := control.Query(checkCtx, "SELECT id, updates FROM "+table+" ORDER BY id")
	if err != nil {
		b.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var id int
		var updates int64
		if err := rows.Scan(&id, &updates); err != nil {
			b.Fatal(err)
		}
		if count >= len(keys) || id != keys[count] || updates != int64(b.N) {
			b.Fatalf("unexpected row (%d, %d): want keys %v with %d updates each", id, updates, keys, b.N)
		}
		count++
	}
	if rows.Err() != nil || count != len(keys) {
		b.Fatalf("expected %d rows, got %d: %v", len(keys), count, rows.Err())
	}
}

func upsertBenchConnect(b *testing.B, admin bool) *pgx.Conn {
	b.Helper()
	host, port := os.Getenv("POSTGRES_HOST"), os.Getenv("POSTGRES_PORT")
	if host == "" {
		host = "::1"
	}
	if port == "" {
		port = "6432"
	}
	params := getConnectionParams()
	if admin {
		params["user"], params["database"] = "spqr-console", "spqr-console"
	}
	dsn := url.URL{
		Scheme: "postgres", Host: net.JoinHostPort(host, port),
		User: url.UserPassword(params["user"], params["password"]), Path: "/" + params["database"],
		RawQuery: "sslmode=disable",
	}
	cfg, err := pgx.ParseConfig(dsn.String())
	if err != nil {
		b.Fatal(err)
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	cfg.ConnectTimeout = upsertBenchTimeout
	ctx, cancel := context.WithTimeout(context.Background(), upsertBenchTimeout)
	defer cancel()
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = conn.Close(ctx)
	})
	if !admin {
		upsertBenchExec(b, conn, "SET __spqr__.engine_v2 TO true")
		upsertBenchExec(b, conn, "SET statement_timeout TO '5s'")
	}
	return conn
}

func upsertBenchExec(b *testing.B, conn *pgx.Conn, query string) {
	b.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), upsertBenchTimeout)
	defer cancel()
	if _, err := conn.Exec(ctx, query); err != nil {
		b.Fatalf("%s: %v", query, err)
	}
}
