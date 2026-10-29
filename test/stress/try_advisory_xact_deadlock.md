# Partial advisory locks with linearized dispatch

Run against a disposable SPQR instance with shards `sh1` and `sh2` (additional
shards are allowed):

```sh
SPQR_ADVISORY_DEADLOCK_DSN='host=127.0.0.1 port=6432 user=regress dbname=regress sslmode=disable' \
  go test -mod=mod ./test/stress -run '^TestTryAdvisoryXactLockDistributedDeadlock$' -v -count=1 -timeout=40s
```

The test is skipped unless the environment variable is set. It deliberately fails
when it observes the distributed deadlock, then cancels the waiting queries and
rolls back both transactions. A statement timeout is only a cleanup safeguard;
the proof is the pair of opposing waits reported by `pg_blocking_pids`.

Both sessions use `linearize_dispatch=true`, which selects the same dispatch path
as `AutoLinearize`. Advisory calls do not currently set `AutoLinearize` themselves.

1. In a transaction, s2 takes lock `684312904` on `sh1`. The shard hint fixes the
   interleaving at the point before s2 takes the lock on `sh2`.
2. In another transaction, s1 scatters `pg_try_advisory_xact_lock(684312904)`.
   It gets `false` on `sh1`, then `true` on `sh2`, despite linearized dispatch.
3. s2 scatters blocking `pg_advisory_xact_lock(684312904)` and waits for s1 on `sh2`.
4. s1 scatters the same blocking call and waits for s2 on `sh1`.

Neither shard sees a local cycle. `pg_try_advisory_xact_lock` alone never waits:
repeating only the try call would keep returning partial failures until a
transaction ends. The blocking calls turn that partial ownership into a deadlock.
