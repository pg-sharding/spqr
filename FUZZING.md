# Extended protocol compatibility — 2026-10-30

Active branch: [fuzz-xproto-compatibility](https://github.com/pg-sharding/spqr/tree/fuzz-xproto-compatibility). It combines the fixes and regressions described below. The opt-in research harness is in `test/xproto/xproto_test.go` as requested.

## Environment

- Original baseline: SPQR master `d3f66eb27ab410aa5493d46f9363a0c029553c91`. The current branch is rebased onto master `1b6680773d922492821759c7062d71b38d729c19`.
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

Representative baseline cases are tracked in `artifacts/baseline/results/`; the remaining combined-case failure is in `artifacts/combined/results/`. Full logs are available in the remote artifact directories listed below and locally as `artifacts/baseline/run.log` and `artifacts/combined/run.log` (ignored by Git).

## Published branches

Each of the following four branches starts at the baseline and contains a test-only commit followed by a separate fix commit. These are the initial commits; subsequent updates are recorded below. All committed tests are appended to `test/xproto/xproto_test.go`.

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
- An additional test-only branch, [test-xproto-empty-query-portal](https://github.com/pg-sharding/spqr/tree/test-xproto-empty-query-portal), commit `53953a66`, adds `TestXProtoEmptyQueryPreservesUnnamedPortal` to `xproto_test.go`. Inside BEGIN, it parses a named statement, binds the unnamed portal, sends `Query("")`, then executes the portal. PostgreSQL returns the row; baseline SPQR returns `34000: portal "" does not exist`. PostgreSQL PASS: `/tmp/spqr-xproto-empty-query-portal/xproto-k1hg8xm7/run.log`; baseline FAIL: `/tmp/spqr-xproto-empty-query-portal/xproto-x5kg80uk/run.log`; lint `0 issues`. The fix is integrated as `9a5dc900`: it invalidates the unnamed portal after parsing a nonempty Simple Query, including whitespace/comment handling, and preserves the bound statement definition/hash when the client later replaces or closes that statement. It builds on the Sync lifecycle fix.

## Second round

The harness now accepts `SPQR_XPROTO_SEED` and `SPQR_XPROTO_CASES`, and tests 15 scenario families. The statement-close/reprepare family uses a fresh portal name, eliminating the invalid duplicate-Bind case from the first round. Five additional families cover unnamed rebind, format changes on unnamed rebind, repeated Describe, closing an unexecuted portal, and describing a completed portal.

On the integrated four-fix version, seed `20261031` with 450 cases produced 450 accepted PostgreSQL scenarios and 60 SPQR mismatches:

- 30 known suspended-portal resumption failures.
- 30 new `Describe` failures after a named portal completed execution: SPQR returns `SPQRU: error describing slice portal: cursor "p" already exists`; PostgreSQL returns the portal's RowDescription. The helper sends Bind again when it should describe the existing portal.

Remote evidence: `/tmp/spqr-xproto-fuzz-v2/xproto-_t78e5l8/`. No transport failure occurred.

Two more minimized tests are published on separate branches based on `d3f66eb2`:

- [test-xproto-describe-completed-portal](https://github.com/pg-sharding/spqr/tree/test-xproto-describe-completed-portal), `6a5eb2d8`: Describe a named portal after Execute and Sync inside BEGIN. PostgreSQL returns RowDescription; SPQR tries to Bind the existing portal again. The integrated fix `cacc5d14` retains the descriptor obtained while deploying the statement for Execute.
- [test-xproto-bound-statement-snapshot](https://github.com/pg-sharding/spqr/tree/test-xproto-bound-statement-snapshot), `4cc9de4d`: bind a portal, close/reparse its statement, then Describe/Execute the original portal. An initial query on shard 2 and a bound query on shard 1 make the missing-statement error deterministic. The statement snapshot in `9a5dc900` fixes this as well. An independent 48-case replacement matrix found 18 failures before the snapshot fix and none afterwards; all 48 cases passed on PostgreSQL.

The updated PR #3150 test (`3d9ba341`, integrated as `4a817184`) combines the positive Sync scenario with a nonempty Simple Query followed by Execute of the former unnamed portal. It checks SQLSTATE `34000`, uses explicit message sequences, and has no table-driven loop.

At integrated commit `4a817184`, the full xproto suite and relay/frontend unit tests pass. The seed `20261101` run accepts all 900 PostgreSQL scenarios and reports exactly 60 mismatches, all in the already known suspended-portal family; the completed-portal Describe failures are gone. Evidence: `/tmp/spqr-xproto-fuzz-v2/xproto-icfvwd1w/`. This run predates the shared Bind-description cache redesign requested for PR #3148.

## Bind description cache and PostgreSQL CI

The `7cd756d6` revision of PR #3148 introduced the encoded Bind cache key with only `DestinationPortal` cleared: statement identity, parameter formats and values, and result formats all participate. NULL and empty parameters remain distinct. Identical Bind messages reuse the ready descriptor across portal names. This supersedes the original portal-name key in `1be9c1ce`.

At the user's request, PR #3148 is narrowed to the test, `bindDescriptionCacheKey`, and the changes required to use that key. It does not include additional portal/cache lifecycle changes. The combined research branch retains its transaction-end cleanup alongside the other lifecycle fixes. The original statement-name invalidation does not remove full-Bind cache keys; revising that policy remains separate from the narrowed PR.

The regression test is formatted like its neighbors and checks text/binary/text descriptions plus a different statement's column name. The full SPQR suite, PostgreSQL regression checks, 24 additional Bind controls on both servers, relay unit tests, cache-key/reuse controls, and lint all pass. Evidence: `/tmp/spqr-xproto-bind-desc-cache/` (`xproto-v4o9a65f`, `xproto-awm10103`, `xproto-trshh7u8`, and the unit/lint logs).

The shared Bind cache is also integrated with the statement snapshot and completed-portal Describe fixes on the fuzz branch. The final combined full xproto suite, relay/frontend unit tests, and lint pass. Repeating seed `20261101` with 900 cases still reports only the 60 known suspended-portal mismatches, with zero rejected reference cases and no new mismatch families. Evidence: `/tmp/spqr-xproto-fuzz-v2/xproto-ywiyfhl2/`.

All newly added regression messages and the compatibility harness now use multiline formatting matching PR #3148. A temporary Go parser check compared the complete ASTs, including comments and excluding source positions, before and after formatting: unchanged. After rebasing the combined branch onto `1b668077`, relay/frontend unit tests and the full xproto suite pass again; the repeated 900-case run has the same 60 known suspended-portal mismatches. Evidence: `/tmp/spqr-xproto-fuzz-v2/xproto-p7yjs5m3/`. The named-Bind test and fix are already in this master, so their duplicate commits were dropped during rebase.

The separate branch [ci-xproto-postgres](https://github.com/pg-sharding/spqr/tree/ci-xproto-postgres), commit `2fb40dcf`, adds a PostgreSQL 17 CI service and runs `go test -tags=xproto` with `SPQR_XPROTO_TEST_PURE_PG_ONLY=1`. The existing `thisIsSPQRSpecificTest` helper supplies all skips; no tests or skip lists were changed. Its intended test command passed remotely: 47 passed, 7 skipped, zero failures. Evidence: `/tmp/spqr-ci-xproto-postgres/pg-yplk0idx/run.log`. The hosted Actions job will run when the branch has a pull request.

Run the harness with a PostgreSQL reference and the SPQR endpoint reachable on port 6432:

```sh
SPQR_XPROTO_REFERENCE_HOST=spqr_shard_1 \
POSTGRES_HOST=regress_router POSTGRES_PASSWORD=12345678 \
SPQR_XPROTO_SEED=20261031 SPQR_XPROTO_CASES=450 \
go test -mod=mod -tags=all ./test/xproto -run '^TestXProtoCompatibilitySweep$' -v
```

The reference connection uses the test cluster's trust authentication. `SPQR_XPROTO_ARTIFACTS` optionally selects where representative mismatch JSON files are written. The harness logs mismatches rather than failing on every difference; a successful harness exit alone does not imply protocol equivalence.

No claim is made about untested protocol sequences or concurrent workloads.
