# Extended protocol compatibility — 2026-10-30

Active branch: [fuzz-xproto-compatibility](https://github.com/pg-sharding/spqr/tree/fuzz-xproto-compatibility). It contains the four fixes, their tests, and the empty-query regression from the separate branches below. The opt-in research harness is now in `test/xproto/xproto_test.go` as requested.

## Environment

- Baseline: SPQR master `d3f66eb27ab410aa5493d46f9363a0c029553c91`.
- Reference: PostgreSQL 17.11, direct connection to shard 1.
- Builds and tests: VM `158.160.212.50`, Go 1.27.2 with `-mod=mod`.
- Each run uses an isolated Docker project with four primary shards and replicas; the runner removes its containers and volumes afterwards.
- Generated scenarios use bounded Parse, Bind, Describe, Execute, Close, Sync, and ordinary Query messages. PostgreSQL filters out invalid lifecycle combinations before SPQR comparison.

## Differential run

The research harness uses seed `20261030` and 200 cases. It varies named/unnamed statements and portals, text/binary parameter and result formats, Sync placement, and portal lifecycle operations. Each case runs on PostgreSQL first, then SPQR. Responses are compared by message type, values, result formats, command tags, SQLSTATE/message, and transaction status. Table OIDs, notices, and ParameterStatus are excluded from comparison.

PostgreSQL rejected 20 cases in `close_statement_reprepare`: closing a statement does not close its existing named portal, so rebinding the same portal name remains invalid. Those cases were excluded from SPQR findings. There were 180 accepted reference cases.

| Scenario | Baseline mismatches | With four fixes |
| --- | ---: | ---: |
| Unnamed Bind, Sync, Execute | 7 | 0 |
| Unnamed Bind, Describe, Sync, Execute | 6 | 0 |
| Named Bind preserving an existing unnamed portal | 20 | 0 |
| Close named portal, Bind same name again | 20 | 0 |
| Describe two portals with different result formats | 11 | 0 |
| Resume a suspended named portal | 20 | 20 |
| **Total** | **84** | **20** |

Counts represent generated cases, not independent bugs. The first two rows have the same root cause. The suspended-portal issue is the previously known repeated Execute lifecycle problem; these four fixes do not address it.

Baseline artifacts: [run log](artifacts/baseline/run.log), representative cases in `artifacts/baseline/results/`.
Combined artifacts: [run log](artifacts/combined/run.log), remaining case in `artifacts/combined/results/`.

## Published branches

Each of the following four branches starts at the baseline and contains a test-only commit followed by a separate fix commit. All committed tests are appended to `test/xproto/xproto_test.go`.

| Branch | Test commit | Fix commit | Behavior |
| --- | --- | --- | --- |
| [fix-xproto-named-bind-preserve-unnamed](https://github.com/pg-sharding/spqr/tree/fix-xproto-named-bind-preserve-unnamed) | `67d61b56` | `86fce44d` | A named Bind must not reset `unnamedPortalExists`. |
| [test-xproto-unnamed-portal-sync](https://github.com/pg-sharding/spqr/tree/test-xproto-unnamed-portal-sync) | `c735f8ab` | `7aff79bc` | Preserve the unnamed portal and plan across Sync inside an active transaction; clear on Simple Query and transaction completion. |
| [test-xproto-close-rebind](https://github.com/pg-sharding/spqr/tree/test-xproto-close-rebind) | `ae37e01f` | `d93a89c3` | Close backend portals on attached shards and drain their replies before acknowledging Close. |
| [test-xproto-portal-result-format](https://github.com/pg-sharding/spqr/tree/test-xproto-portal-result-format) | `f1593c8e` | `1be9c1ce` | Cache descriptions by portal name, with Bind/Close/transaction invalidation. |

Every minimized test passes on PostgreSQL, fails on the baseline, and passes with its corresponding fix. Every fix passed the full xproto suite and relay unit tests; the Sync fix also passed frontend unit tests. Lint reported `0 issues` for the affected packages. Six supplementary Close cases (unexecuted, virtual, missing, unnamed, multiple attached shards, suspended) passed on PostgreSQL and fixed SPQR.

The temporary combined worktree also passed the full xproto suite: 74 ordinary tests plus the differential harness. This combined worktree is only a verification artifact; it has not been published as another branch. The independent branches touch shared code, so merging them may require resolving overlapping changes.

## Remote artifacts

- Initial sweep: `/tmp/spqr-xproto-compat-KYCjm8/xproto-633uffad/`.
- All four minimized tests on PostgreSQL: `/tmp/spqr-xproto-compat-KYCjm8/xproto-vrgbdl2u/`.
- All four minimized tests on baseline: `/tmp/spqr-xproto-compat-KYCjm8/xproto-xksg3ulv/`.
- Named Bind fixed full suite: `/tmp/spqr-xproto-named-bind/xproto-gyqo43tp/`.
- Unnamed Sync fixed full suite: `/tmp/spqr-xproto-unnamed-sync/xproto-sktv7rbs/`.
- Close fixed full suite: `/tmp/spqr-xproto-close-rebind/xproto-5uwxqyns/`.
- Portal format fixed full suite: `/tmp/spqr-xproto-portal-format/xproto-j0idby7d/`.
- Combined full suite and sweep: `/tmp/spqr-xproto-combined/xproto-9tqt14ut/`.

## Follow-up

- A named portal resumed after `PortalSuspended` currently reaches `SPQRU: cannot dispatch query slice without a plan`. The plan is cleared after Execute, and preserving the plan alone is insufficient because the executor also resends Bind. This needs separate portal execution lifecycle work.
- An additional test-only branch, [test-xproto-empty-query-portal](https://github.com/pg-sharding/spqr/tree/test-xproto-empty-query-portal), commit `53953a66`, adds `TestXProtoEmptyQueryPreservesUnnamedPortal` to `xproto_test.go`. Inside BEGIN, it parses a named statement, binds the unnamed portal, sends `Query("")`, then executes the portal. PostgreSQL returns the row; baseline SPQR returns `34000: portal "" does not exist`. PostgreSQL PASS: `/tmp/spqr-xproto-empty-query-portal/xproto-k1hg8xm7/run.log`; baseline FAIL: `/tmp/spqr-xproto-empty-query-portal/xproto-x5kg80uk/run.log`; lint `0 issues`. No fix is included: correct handling builds on the Sync lifecycle change and must invalidate the unnamed portal only after parsing a nonempty Simple Query. Checking only `query == ""` would miss whitespace/comments. This issue is separate from the four fixes above.

## Second round

The harness now accepts `SPQR_XPROTO_SEED` and `SPQR_XPROTO_CASES`, and tests 15 scenario families. The statement-close/reprepare family uses a fresh portal name, eliminating the invalid duplicate-Bind case from the first round. Five additional families cover unnamed rebind, format changes on unnamed rebind, repeated Describe, closing an unexecuted portal, and describing a completed portal.

On the integrated four-fix version, seed `20261031` with 450 cases produced 450 accepted PostgreSQL scenarios and 60 SPQR mismatches:

- 30 known suspended-portal resumption failures.
- 30 new `Describe` failures after a named portal completed execution: SPQR returns `SPQRU: error describing slice portal: cursor "p" already exists`; PostgreSQL returns the portal's RowDescription. The helper sends Bind again when it should describe the existing portal.

Remote evidence: `/tmp/spqr-xproto-fuzz-v2/xproto-_t78e5l8/`. No transport failure occurred. An independent 48-case statement replacement matrix also identified a missing-prepared-statement error; minimization and fixes are in progress.

Run the harness with a PostgreSQL reference and the SPQR endpoint reachable on port 6432:

```sh
SPQR_XPROTO_REFERENCE_HOST=spqr_shard_1 \
POSTGRES_HOST=regress_router POSTGRES_PASSWORD=12345678 \
SPQR_XPROTO_SEED=20261031 SPQR_XPROTO_CASES=450 \
go test -mod=mod -tags=all ./test/xproto -run '^TestXProtoCompatibilitySweep$' -v
```

The reference connection uses the test cluster's trust authentication. `SPQR_XPROTO_ARTIFACTS` optionally selects where representative mismatch JSON files are written. The harness logs mismatches rather than failing on every difference; a successful harness exit alone does not imply protocol equivalence.

No claim is made about untested protocol sequences or concurrent workloads.
