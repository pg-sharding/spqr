---
name: spqr-tests
description: Use when adding or changing tests in pg-sharding/spqr, or when a change adds a feature or fixes a bug and needs test coverage. Picks the test type (regress, feature, isolation, xproto, driver, unit) and shows how to write each.
---

# Tests in SPQR

Tests are maintained by people. Every test line is a cost. Write the shortest test that proves the behavior, of exactly one type: the cheapest one that can reproduce it.

## Which test

| Change | Test |
|---|---|
| New or changed console command, `SHOW` column, `HELP` text | regress, `test/regress/tests/console` |
| Query routing, transactions, COPY, multi-shard behavior | regress, `test/regress/tests/router` |
| Coordinator-only behavior | regress, `test/regress/tests/coordinator` |
| Pooler behavior | regress, `test/regress/tests/pooler` |
| Needs several hosts or lifecycle events: coordinator + routers, restarts, failover, etcd, move/redistribute, auth (scram, ldap, krb), config reload, spqrdump | feature, `test/feature/features/*.feature` |
| Concurrency: two sessions interleaving, locks, 2PC, move under load | isolation, `test/isolation/specs` |
| Wire-protocol messages and their exact order (Parse/Bind/Execute, COPY sub-protocol, malformed packets) | xproto, `test/xproto` |
| Bug seen only with a specific driver (JDBC, GORM, Hibernate) | driver test, `test/drivers/<driver>-regress` |
| Bug fix | test of the type above that fails without the fix |
| Pure function hard to reach from SQL (parser corner case, hashing, bound comparison, config parsing) | unit test, table-driven, in the existing `_test.go` next to the code |
| Refactoring with no behavior change | no new tests |

Go up the table only when the cheaper type cannot reproduce the behavior. A feature test costs minutes of CI and a cluster restart; do not write one for something a regress test can show. Never add stress tests or benchmarks in a contributor PR.

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

## Writing a feature test

BDD scenarios on godog, in `test/feature/features/`. Each scenario brings up a docker compose cluster, so they are slow.

- Add a `Scenario` to the existing `.feature` file for the area (`coordinator.feature`, `move.feature`, `reload.feature`, ...). A new file only for a new area.
- Use only existing steps from `test/feature/spqr_test.go`. Do not add Go step definitions unless no combination of existing ones can express the check.
- One scenario checks one behavior. No copy-paste of a neighbouring scenario with a single changed line: extend that scenario instead.
- Compare results with `json_exactly`, after `ORDER BY`. Hide unstable fields with `hide "<field>" field`.
- Reuse configs from `test/feature/conf/`. A new config only if no existing one fits.

```gherkin
  Scenario: SHOW sequences lists sequence created through coordinator
    Given cluster environment is
    """
    ROUTER_CONFIG=/spqr/test/feature/conf/router_cluster.yaml
    """
    Given cluster is up and running
    When I run SQL on host "coordinator"
    """
    REGISTER ROUTER r1 ADDRESS "[regress_router]:7000";
    CREATE REFERENCE TABLE t AUTO INCREMENT id;
    SHOW sequences;
    """
    Then command return code should be "0"
    And SQL result should match json_exactly
    """
    [{"name": "t_id", "value": "0"}]
    """
```

Run one file:

```bash
GODOG_FEATURE_DIR=generatedFeatures GODOG_FEATURE="sequence.feature" make feature_test
```

## Writing an isolation test

`isolationtester` specs: `test/isolation/specs/<name>.spec`, expected output `test/isolation/expected/<name>.out`, and `test: <name>` in `test/isolation/schedule`.

- Two sessions, a few steps each, as few permutations as prove the race. Every extra permutation multiplies the expected output.
- Name steps `<session>_<action>`, one statement per step.
- Generate expected output with `make isolation_regress`, do not write it by hand.

## Writing an xproto test

Go tests in `test/xproto` that send raw `pgproto3` messages and compare the exact backend messages. Use only when the order or content of protocol messages is the point; anything visible from psql belongs in regress.

- Add a case to the existing table in `proto_test.go` (or `copy_proto_test.go` for COPY) instead of a new test function.
- Run with `make xproto_regress`.

## Driver tests

`test/drivers/{jdbc,gorm,hibernate}-regress`, run with `make jdbc_regress`, `make gorm_regress`, `make hibernate_regress`. Add a case only for a bug that reproduces with that driver and not with psql or xproto.
