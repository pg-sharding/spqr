# Distributed upsert stress benchmark

Run against a disposable xproto environment with shards `sh1` through `sh4`:

```sh
POSTGRES_HOST=127.0.0.1 POSTGRES_PORT=6432 \
POSTGRES_USER=regress POSTGRES_DB=regress POSTGRES_PASSWORD=12345678 \
go test -tags=xproto ./test/xproto -run '^$' \
  -bench '^BenchmarkDistributedUpsert$' -benchtime=10000x -count=1 -timeout=10m
```

The existing `TestMain` initializes the xproto schema. The benchmark additionally
creates its own distribution, key ranges and table, and removes them on exit.
The console connection uses the same endpoint and the `spqr-console` user/database,
as does the existing suite setup.

`Extended` uses parameterized Parse/Bind/Execute/Sync through `pgconn.ExecParams`.
It has `Auto` (the default automatic upsert linearization) and `Forced`
(`linearize_dispatch=true`) cases. The forced case also retains automatic upsert
protection. Both cases must complete every operation successfully.

Eight concurrent clients repeatedly upsert the same four keys, one on each
shard, rotating their order in an IN list. Each operation is one autocommit
`INSERT ... SELECT ... ON CONFLICT DO UPDATE`: the SELECT reads each shard's
local row, without requiring multi-VALUES rewriting in extended protocol.
Setup, connection establishment and final
verification are excluded from timing. Each final row must contain exactly
`b.N` increments; SQL errors, a missing completion, a non-idle transaction or
an incorrect counter fail the benchmark. The benchmark uses a 5-second server
statement timeout and a 10-second client deadline per operation. Timeouts are
failures, not successful deadlock recovery.

Use `-bench 'BenchmarkDistributedUpsert/Extended/Auto$'` or
`-bench 'BenchmarkDistributedUpsert/Extended/Forced$'` to select one case.
Increase `-benchtime` or use `-count` for a longer run. The initial calibration
may use fewer than eight clients when `b.N < 8`.
