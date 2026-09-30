# spqr-monitor

`spqr-monitor check` looks for logical corruption: rows stored on a shard that
does not own their distribution key according to SPQR's etcd metadata. It reads
table data without changing it and stops at the first offending row. It does not
check PostgreSQL pages, indexes, or checksums, or detect missing rows.

## Build and configure

From the repository root:

```sh
make build_monitor
```

Create `shard-data.yaml` with every shard you want to check. Shard IDs must match
SPQR metadata; replace the example hosts, database, and credentials with yours.
The database user needs access to the distributed tables and `SELECT` privilege on them.

```yaml
shards:
  sh1:
    hosts: ["localhost:5432"]
    db: app
    usr: monitor
    pwd: "replace-me"
  sh2:
    hosts: ["localhost:5433"]
    db: app
    usr: monitor
    pwd: "replace-me"
```

The monitor needs access to etcd and the configured PostgreSQL hosts. It prefers
a standby when available, so replication lag can affect results. For a check
against primaries, list only primary endpoints in this file.

## Find misplaced rows

Run one check with a report path that does **not** already exist:

```sh
./spqr-monitor check \
  --shard-data ./shard-data.yaml \
  --etcd-addr localhost:2379 \
  --file ./corruption-report.txt \
  --tablesample-size 100
```

Use your cluster's etcd client endpoint; repeat `--etcd-addr` for multiple
endpoints. The built-in default is `localhost:2389`.

`--tablesample-size` is a **percentage**, using PostgreSQL `TABLESAMPLE SYSTEM`:
`100` checks all table blocks until a match is found, `1` samples approximately
1% of blocks, and the default is only `0.01`%. Smaller samples can miss misplaced
rows, especially in small tables. A 100% check can be expensive on large tables.

Interpret the printed result:

| Output | Meaning |
| --- | --- |
| `0;OK` | No offending row was found in the data checked; see the limitations below. |
| `2;corruption found, check "..." file` | A row was found, or the report file already exists. |
| `2;...` with an error message | The check failed; investigate the reported error. |
| `0;...skipping...` | The check did not run because configuration or credentials were missing/invalid. |

These prefixes are printed status values, **not process exit codes**. `check`
normally exits with code 0 even after reporting corruption or a runtime error;
automation must inspect the output.

A newly written report contains the row values, relation, and actual shard, for
example:

```text
Corruption found: row [1 001], rel "xMove" shard "sh2"
```

If key `1` belongs on `sh1`, this identifies a misplaced row on `sh2`. Compare
the report with the current key-range metadata before deciding how to repair it.
The monitor does not repair rows or list every mismatch.

**The report is a persistent alarm:** even an empty existing file prevents a new
scan. After investigating, archive/remove it or choose a new `--file` path for
the next check. Do not pre-create the file; its parent directory must be writable.

## Check through a router

To access shards through SPQR, add the router connection flags:

```sh
./spqr-monitor check \
  --shard-data ./shard-data.yaml \
  --etcd-addr localhost:2379 \
  --file ./router-corruption-report.txt \
  --tablesample-size 100 \
  --host localhost --port 6432 \
  --user monitor --database app --password 'replace-me'
```

The shard configuration is still required to select shards. The monitor targets
each shard using `SET __spqr__execute_on`. If `--user` is omitted, it uses the
credentials and database from each shard's configuration.

## Limitations and related commands

- **Current range-boundary limitation:** `getQDBData` in [main.go](main.go) leaves
  upper bounds unset. Rows above a shard's assigned ranges can therefore go
  undetected even at 100% sampling. For example, with `sh1` owning `[0, 10)` and
  `sh2` owning `[10, +infinity)`, key `20` on `sh1` is missed. Treat `0;OK` as a
  diagnostic result, not proof that all rows are correctly placed.
- Only configured shards and relations attached to distributions with key ranges
  are checked. Missing tables are skipped. Multidimensional key ranges are not
  supported by the condition builder.
- Run with key-range moves paused/completed and metadata stable: the check does
  not exclude locked ranges or coordinate its snapshot with transfers.
- `verify --key-range <id>` checks source/destination row counts for a key range
  belonging to a move task. Equal counts do not prove equal contents; it does
  not unlock the range.
- `recover --dry-run` previews recovery of locked ranges in failed move task
  groups. Without `--dry-run`, `recover` can unlock ranges and delete task/move
  metadata via etcd and the coordinator (`--coordinator-addr`, default
  `localhost:7003`). It is not a general data-corruption repair command.

See `./spqr-monitor check --help` for flags and
[monitor.feature](../../test/feature/features/monitor.feature) for executable
usage examples.
