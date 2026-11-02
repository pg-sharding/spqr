---
name: spqr-tests
description: Use when adding or changing tests in pg-sharding/spqr, or when a change adds a feature or fixes a bug and needs test coverage. Decides between regress and unit tests and shows how to write a regress test.
---

# Tests in SPQR

Tests are maintained by people. Every test line is a cost. Write the shortest test that proves the behavior.

## Which test

| Change | Test |
|---|---|
| New or changed console command, `SHOW` column, `HELP` text | regress, `test/regress/tests/console` |
| Query routing, transactions, COPY, multi-shard behavior | regress, `test/regress/tests/router` |
| Coordinator-only behavior | regress, `test/regress/tests/coordinator` |
| Pooler behavior | regress, `test/regress/tests/pooler` |
| Bug fix | regress test that fails without the fix |
| Pure function hard to reach from SQL (parser corner case, hashing, bound comparison, config parsing) | unit test, table-driven, in the existing `_test.go` next to the code |
| Refactoring with no behavior change | no new tests |

Do not write unit tests for:

- getters, setters, struct conversions, formatting helpers
- code paths already executed by the regress test you added
- every backend × every operation combination — one representative case is enough
- mocks that only check a function was called

Do not edit existing tests to make them pass with your change unless the behavior change itself requires it. If an existing test breaks because output became non-deterministic, the change is wrong, not the test.

## Writing a regress test

Layout:

```
test/regress/schedule/<suite>          # list of tests, one "test: <name>" per line
test/regress/tests/<suite>/sql/<name>.sql
test/regress/tests/<suite>/expected/<name>.out
```

Prefer adding a few statements to an existing file that already covers the area (e.g. `console/sql/show_key_ranges.sql` for key range `SHOW` output). Create a new file only for a new area; then add `test: <name>` to `test/regress/schedule/<suite>`.

Rules:

- 5–30 lines of SQL. Only statements that prove the new behavior plus the minimal setup.
- Deterministic output: `ORDER BY` in every `SHOW` with several rows; never print timestamps, PIDs, ports or random ids — select the columns that are stable.
- Clean up at the end: `DROP DISTRIBUTION ALL CASCADE;` in console tests, `SELECT __spqr__console_execute('DROP DISTRIBUTION ALL CASCADE');` in router tests.
- No comments in SQL except one line when output stability needs explaining.

Console example (`test/regress/tests/console/sql/<name>.sql`):

```sql
CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
CREATE KEY RANGE krid1 FROM 10 ROUTE TO sh1 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid2 FROM 20 ROUTE TO sh2 FOR DISTRIBUTION ds1;

SHOW key_ranges ORDER BY key_range_id;

DROP DISTRIBUTION ALL CASCADE;
```

Router example (`test/regress/tests/router/sql/<name>.sql`):

```sql
SELECT __spqr__console_execute('CREATE DISTRIBUTION d(int)');
SELECT __spqr__console_execute('CREATE KEY RANGE k1 FROM 100 ROUTE TO sh2');
SELECT __spqr__console_execute('CREATE KEY RANGE k0 FROM 0 ROUTE TO sh1');
SELECT __spqr__console_execute('CREATE RELATION t(i)');

CREATE TABLE t(i int);
INSERT INTO t(i) VALUES (1);
INSERT INTO t(i) VALUES (101);

/* __spqr__execute_on: sh1 */ SELECT * FROM t ORDER BY i;
/* __spqr__execute_on: sh2 */ SELECT * FROM t ORDER BY i;

DROP TABLE t;
SELECT __spqr__console_execute('DROP DISTRIBUTION ALL CASCADE');
```

## Expected output

Generate it, do not invent it:

```bash
make regress
```

pg_regress writes actual output to `test/regress/tests/<suite>/results/<name>.out` inside the container and prints the diff. Check the output is correct, then save it as `expected/<name>.out`. If you cannot run Docker, write `expected/<name>.out` by copying the exact psql formatting of neighbouring `.out` files and let CI confirm it; do not mention this in the PR description.
