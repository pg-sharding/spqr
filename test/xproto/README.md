# Extended protocol tests

The `proto-extended-postgres` CI job runs the PostgreSQL-compatible xproto tests
against PostgreSQL 17. `proto-extended-odyssey-master` runs the same tests through
Odyssey, built from `yandex/odyssey` master, with PostgreSQL 17 as its backend.
The Odyssey configuration in `conf/odyssey.conf` enables transaction pooling and
`pool_reserve_prepared_statement yes` to exercise prepared statement handling
across backend attachments. Both jobs use `SPQR_XPROTO_TEST_PURE_PG_ONLY=1` to
skip SPQR-specific routing tests.

The Odyssey job saves the tested Odyssey commit, pooler and PostgreSQL logs,
and the test output in the `xproto-odyssey-logs` artifact, including on failure.

## Distributed upsert stress benchmark

CI runs both benchmark cases after the tests in the `proto-extended` job.

Run against a disposable xproto environment with shards `sh1` through `sh4`:

```sh
POSTGRES_HOST=127.0.0.1 POSTGRES_PORT=6432 \
POSTGRES_USER=regress POSTGRES_DB=regress POSTGRES_PASSWORD=12345678 \
timeout 10m go test -tags=xproto ./test/xproto -run '^$' \
  -bench '^BenchmarkDistributedUpsert$' -benchtime=10000x -count=1
```

`Extended/Auto` uses automatic upsert linearization; `Extended/Forced` also sets
`linearize_dispatch=true`. Up to eight concurrent clients execute parameterized
`INSERT ... SELECT ... ON CONFLICT DO UPDATE` through `pgconn.ExecParams`, updating
one row per shard. SQL errors fail the benchmark, and every row must receive
exactly `b.N` increments. The benchmark creates and removes its own table and
distribution. Setup and final verification are excluded from timing.

The external `timeout` command limits the full run; Go's `-timeout` flag does not
apply to benchmarks.
