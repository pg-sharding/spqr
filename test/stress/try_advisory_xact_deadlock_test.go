package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// This opt-in reproducer fails after observing both edges of a distributed deadlock.
func TestTryAdvisoryXactLockDistributedDeadlock(t *testing.T) {
	dsn := os.Getenv("SPQR_ADVISORY_DEADLOCK_DSN")
	if dsn == "" {
		t.Skip("set SPQR_ADVISORY_DEADLOCK_DSN to a disposable router with sh1 and sh2")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	connect := func() *pgx.Conn {
		cfg, err := pgx.ParseConfig(dsn)
		require.NoError(t, err)
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
		conn, err := pgx.ConnectConfig(ctx, cfg)
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close(context.Background()) })
		return conn
	}
	exec := func(conn *pgx.Conn, query string) {
		t.Helper()
		_, err := conn.Exec(ctx, query)
		require.NoError(t, err, query)
	}
	onShard := func(query string, shard int) string {
		return fmt.Sprintf("%s /* __spqr__execute_on: sh%d */", query, shard)
	}
	s1, s2, observer := connect(), connect(), connect()
	for _, conn := range []*pgx.Conn{s1, s2, observer} {
		exec(conn, "SET __spqr__engine_v2 TO on")
		exec(conn, "SET __spqr__advisory_lock_behaviour TO SCATTER")
		// Use the same dispatch ordering as AutoLinearize even for advisory calls.
		exec(conn, "SET __spqr__linearize_dispatch TO true")
		exec(conn, "SET statement_timeout TO '10s'")
	}
	pids := [2][2]int32{}
	for i, conn := range []*pgx.Conn{s1, s2} {
		exec(conn, "BEGIN")
		t.Cleanup(func() { _, _ = conn.Exec(context.Background(), "ROLLBACK") })
		for shard := 1; shard <= 2; shard++ {
			require.NoError(t, conn.QueryRow(ctx, onShard("SELECT pg_backend_pid()", shard)).Scan(&pids[i][shard-1]))
		}
	}

	const lock = "684312904"
	// Stop s2 after acquiring the first shard's lock, before it reaches sh2.
	exec(s2, onShard("SELECT pg_advisory_xact_lock("+lock+")", 1))
	rows, err := s1.Query(ctx, "SELECT pg_try_advisory_xact_lock("+lock+")")
	require.NoError(t, err)
	acquired, err := pgx.CollectRows(rows, pgx.RowTo[bool])
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(acquired), 2)
	require.False(t, acquired[0])
	require.True(t, acquired[1])
	t.Logf("s1 try-lock results in shard order: %v", acquired)

	startLock := func(conn *pgx.Conn, backendPIDs [2]int32) {
		done := make(chan struct{})
		go func() {
			defer close(done)
			_, _ = conn.Exec(ctx, "SELECT pg_advisory_xact_lock("+lock+")")
		}()
		t.Cleanup(func() {
			// Cancel the blocked backend before rolling back the whole transaction.
			for shard, pid := range backendPIDs {
				_, _ = observer.Exec(context.Background(), onShard(fmt.Sprintf("SELECT pg_cancel_backend(%d)", pid), shard+1))
			}
			<-done
			_, _ = conn.Exec(context.Background(), "ROLLBACK")
		})
	}
	waitForBlock := func(shard int, waiter, holder int32) {
		t.Helper()
		require.Eventually(t, func() bool {
			var blockers []int32
			err := observer.QueryRow(ctx, onShard(fmt.Sprintf("SELECT pg_blocking_pids(%d)", waiter), shard)).Scan(&blockers)
			return err == nil && len(blockers) == 1 && blockers[0] == holder
		}, 3*time.Second, 10*time.Millisecond, "sh%d: backend %d must wait for %d", shard, waiter, holder)
		t.Logf("sh%d: backend %d waits for backend %d", shard, waiter, holder)
	}

	startLock(s2, pids[1])
	waitForBlock(2, pids[1][1], pids[0][1])
	startLock(s1, pids[0])
	waitForBlock(1, pids[0][0], pids[1][0])
	// Recheck the first edge: both waits must exist at the same time.
	waitForBlock(2, pids[1][1], pids[0][1])
	t.Fatal("distributed deadlock with linearize_dispatch=true: s1 waits for s2 on sh1; s2 waits for s1 on sh2")
}
