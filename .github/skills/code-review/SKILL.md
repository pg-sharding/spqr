---
name: code-review
description: Review SPQR pull requests for correctness, data safety, concurrency, PostgreSQL protocol behavior, sharding and metadata consistency, compatibility, and adequate tests. Use for every pull request code review in this repository.
---

# SPQR pull request review

Follow the repository-wide instructions in `.github/copilot-instructions.md`. Review for high-signal defects introduced by the pull request; do not duplicate formatter, linter, or generated-file feedback.

## Gather context

1. Read the pull request description and any linked issues.
2. Inspect the changed code, its callers, interfaces, tests, and the nearest analogous implementation.
3. Use the GitHub MCP server when useful to inspect linked issues, relevant workflow failures, commit history, or an upstream commit range.
4. For dependency updates, inspect the upstream changes between the old and new revisions and identify behavior or API changes that affect SPQR. Verify `go.mod`, `go.sum`, and vendored sources remain consistent.
5. Treat passing CI as evidence, not proof. Check whether the changed behavior is actually exercised by the relevant test suite.

## Review priorities

### Routing and SQL semantics

- Check key-range boundaries, hash/range distribution, shard selection, scatter/gather behavior, result merging, and empty or overlapping ranges.
- Check parser and planner changes against PostgreSQL semantics, including prepared statements, bind parameters, multi-statement queries, transaction state, errors, cancellation, and connection reuse.
- Make sure routing decisions cannot silently send reads or writes to the wrong shard.

### Transactions, concurrency, and lifecycle

- Check atomicity, idempotency, retry behavior, and recovery for multi-shard transactions and 2PC.
- Check lock ordering, goroutine lifetimes, channel ownership/closure, context cancellation, races, and state transitions.
- Check that every acquired client, shard connection, transaction, timer, and other resource is released on success, error, cancellation, and panic paths.
- Pay special attention to pool accounting and to code that can leave a connection in a dirty PostgreSQL transaction or protocol state.

### Metadata and distributed state

- Keep QDB interface behavior consistent across in-memory and etcd-backed implementations.
- Check compare-and-swap/version handling, partial failures, stale reads, retries, watches, and coordinator/router convergence.
- Require safe migration and mixed-version behavior when persistent metadata, protobufs, public APIs, or configuration schemas change.

### Compatibility and security

- Preserve existing YAML/TOML defaults, CLI flags, PostgreSQL client compatibility, and public Go/protobuf APIs unless the pull request explicitly documents a breaking change.
- Check authentication, authorization, TLS, credential/log handling, input validation, and denial-of-service risks on externally reachable paths.
- For generated protobufs, parser output, mocks, or documentation, verify the source definition and regeneration path are updated rather than reviewing generated churn in isolation.

### Tests

- Require a regression test for bug fixes and focused tests for new behavior, boundary conditions, and error paths.
- Ask for race coverage when synchronization, pooling, cancellation, or shared state changes.
- Match the test to the affected layer: unit tests for local logic, feature/regression tests for SQL-visible behavior, and integration tests for QDB, drivers, or distributed workflows.
- Prefer the narrowest relevant Make target while ensuring the affected behavior is covered.

## Comment policy

Only leave an inline comment when there is a concrete, actionable problem introduced by the pull request.

Each finding should state:

- the failure scenario or violated invariant;
- the likely impact;
- the smallest reasonable fix or test that would prove the behavior.

Do not:

- request cosmetic refactors or personal style preferences;
- repeat issues already enforced by `gofmt`, `goimports`, or `golangci-lint`;
- flag pre-existing problems outside the changed behavior;
- speculate without a realistic execution path;
- treat Copilot review as a substitute for a human approval.

If no actionable defect is found, say so clearly and mention any important coverage limitation in the review summary.
