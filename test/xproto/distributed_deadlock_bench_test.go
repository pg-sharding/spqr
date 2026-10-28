//go:build xproto || all

package prep_stmt_test

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// BenchmarkDistributedUpsert runs concurrent extended-protocol upsert queries on four shards.
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

	for _, linearize := range []bool{false, true} {
		name := "Auto"
		if linearize {
			name = "Forced"
		}
		b.Run("Extended/"+name, func(b *testing.B) {
			benchmarkDistributedUpsert(b, control, table, linearize)
		})
	}
}

func benchmarkDistributedUpsert(b *testing.B, control *pgx.Conn, table string, linearize bool) {
	b.StopTimer()
	upsertBenchExec(b, control, "TRUNCATE "+table)
	upsertBenchExec(b, control, "INSERT INTO "+table+" (id, updates) VALUES (1, 0), (12, 0), (1001, 0), (2001, 0)")

	// Rotate the key order across clients and iterations. INSERT SELECT reads
	// each shard's local key and needs no multi-VALUES rewriting in extended
	// protocol. One key per shard avoids intra-shard row-order deadlocks.
	keys := []int{1, 12, 1001, 2001}
	parameters := make([][][]byte, len(keys))
	for offset := range keys {
		parameters[offset] = make([][]byte, len(keys))
		for i := range keys {
			parameters[offset][i] = []byte(strconv.Itoa(keys[(i+offset)%len(keys)]))
		}
	}
	parameterized := fmt.Sprintf(
		"INSERT INTO %s (id, updates) SELECT id, 1 FROM %s WHERE id IN ($1, $2, $3, $4) "+
			"ON CONFLICT (id) DO UPDATE SET updates = %s.updates + 1", table, table, table)

	clients := make([]*pgx.Conn, min(8, b.N))
	for i := range clients {
		clients[i] = upsertBenchConnect(b, false)
		upsertBenchExec(b, clients[i], fmt.Sprintf("SET __spqr__.linearize_dispatch TO %t", linearize))
	}

	ctx := context.Background()
	var workers sync.WaitGroup
	b.ResetTimer()
	b.StartTimer()
	for worker, conn := range clients {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for iteration := worker; iteration < b.N; iteration += len(clients) {
				offset := (iteration/len(clients) + worker) % len(keys)
				_, err := conn.PgConn().ExecParams(ctx, parameterized,
					parameters[offset], nil, nil, nil).Close()
				if err != nil {
					b.Errorf("client %d, operation %d: %v", worker, iteration, err)
					return
				}
			}
		}()
	}
	workers.Wait()
	b.StopTimer()
	if b.Failed() {
		return
	}

	rows, err := control.Query(ctx, "SELECT id, updates FROM "+table+" ORDER BY id")
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

func BenchmarkDistributedBatchInsert(b *testing.B) {
	if isPurePGTesting() {
		b.Skip("requires the xproto sharding environment")
	}
	b.StopTimer()
	admin := upsertBenchConnect(b, true)
	control := upsertBenchConnect(b, false)
	table := fmt.Sprintf("xproto_batch_%x", time.Now().UnixNano())
	upsertBenchExec(b, admin, "CREATE RELATION "+table+" (shard_key) IN ds1")
	upsertBenchExec(b, control, "CREATE TABLE "+table+" (shard_key integer, id integer, PRIMARY KEY (shard_key, id))")
	b.Cleanup(func() {
		upsertBenchExec(b, control, "DROP TABLE "+table)
		upsertBenchExec(b, admin, "ALTER DISTRIBUTION ds1 DETACH RELATION "+table)
	})

	clients := make([]*pgx.Conn, min(8, b.N))
	for i := range clients {
		clients[i] = upsertBenchConnect(b, false)
	}
	ctx := context.Background()
	var workers sync.WaitGroup
	b.ResetTimer()
	b.StartTimer()
	for worker, conn := range clients {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for iteration := worker; iteration < b.N; iteration += len(clients) {
				// Clients contend on the same fresh key on both shards.
				key := iteration / len(clients)
				query := fmt.Sprintf("INSERT INTO %s (shard_key, id) VALUES (1, %d), (12, %d) ON CONFLICT DO NOTHING /* client %d */", table, key, key, worker)
				if _, err := conn.Exec(ctx, query); err != nil {
					b.Errorf("client %d, operation %d: %v", worker, iteration, err)
					return
				}
			}
		}()
	}
	workers.Wait()
	b.StopTimer()
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
	ctx := context.Background()
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_ = conn.Close(ctx)
	})
	if !admin {
		upsertBenchExec(b, conn, "SET __spqr__.engine_v2 TO true")
	}
	return conn
}

func upsertBenchExec(b *testing.B, conn *pgx.Conn, query string) {
	b.Helper()
	if _, err := conn.Exec(context.Background(), query); err != nil {
		b.Fatalf("%s: %v", query, err)
	}
}
