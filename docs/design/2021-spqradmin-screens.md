# SPQR Admin: required screens and behavior

| Field | Value |
| --- | --- |
| Status | Draft |
| Parent design | [SPQR Admin design](2021-spqradmin-design.md) |
| Visual concepts | [Atlas, Transfer Desk, and Control Map](2021-spqradmin-visual-concepts.md) |
| Related issue | [pg-sharding/spqr#2021](https://github.com/pg-sharding/spqr/issues/2021) |
| Last updated | 2026-08-26 |

This document defines the required product surfaces and interactions independently of the selected visual concept. Range-density behavior follows the parent design's [semantic-zoom model](2021-spqradmin-design.md#scale-model-hundreds-first).

## Application shell

```text
SPQR Admin   Cluster: production-eu   Updated 14:32:08   [Refresh] [Present]
             Overview | Activity | Raw data
```

The selected cluster, distribution, range, task, viewport, and presentation state are encoded in the URL. Every data surface shows its observation time and whether it is current or stale.

## Required screens

| Screen | Must answer | Required content |
| --- | --- | --- |
| Cluster overview | What topology or work needs attention? | Coordinator connectivity; shard, distribution, and range counts; active/failed moves; unexpected locks; compact distribution allocation rows |
| Distribution map | What range belongs where? | Selector, `Ordered ranges` / `Proportional key space` mode when valid, minimap, exact viewport, shard legend, search/jump, lock/error overlays, range detail |
| Activity | What is planned, running, stale, or failed? | Exact group state, timestamp, IDs/issuer when available, range/distribution, source → destination, derived phase plus raw state, lock impact, one-line error, and an explicit progress unit such as `4 / 8 batches` or `120k / 500k rows`—otherwise no percentage |
| Task detail | What failed and what is known? | Combined phase timeline; source/destination evidence; application impact; documentation link and copyable recovery command |
| Key-range detail | What are the exact routing facts? | ID, distribution, column types, lower/next bound, shard mapping, lock/matching task, neighbors, relations, relevant raw values |
| Raw data | What did the coordinator report? | Read-only gRPC payloads rendered as familiar `SHOW`-shaped tables, observation timestamp, coordinator identity, TSV/JSON copy; no unrestricted SQL |

Do not add generic infrastructure KPIs merely to fill the cluster overview.

## Distribution-map behavior

- Keep the full-distribution overview and exact viewport synchronized.
- Hover or focus reveals the exact interval and shard; click pins the selection.
- Arrow keys move between adjacent ranges in ordinal order.
- Selecting a task highlights its range without replacing the shard-ownership color.
- Locked, selected, active, failed, and inconsistent ranges stay individually visible even when the ownership layer is binned.
- Direct jump by range ID, bound, shard, or ordinal position must not require panning through preceding ranges.
- The raw table and graphical map expose the same order and values.

Acceptance cases are 1, 12, 240, 1,842, and 10,000 ranges, including locks and failures at the first, middle, and last positions.

## Activity behavior

Order rows by:

1. task group `ERROR`;
2. stale task group `RUNNING`;
3. other task groups `RUNNING`;
4. task groups `PLANNED`;
5. terminal history, only if a future contract retains it.

The shell may show derived navigation summaries, but never present them as SPQR or key-range states:

| Summary | Meaning |
| --- | --- |
| No active work | No active task group or unexpected lock in the current snapshot |
| Moving | A task group is `RUNNING` or a matching move/transfer journal is active |
| Attention | Group `ERROR`, stale active task, inconsistent journals, or `LOCKED` range without a matching move |
| Disconnected | App cannot reach the coordinator; retain the last snapshot behind a stale banner |

Each summary links to its contributing objects and raw states.

## Failure explanation

Task detail leads with plain language tied to evidence, for example:

> Move failed while deleting copied rows from `shard-01`. The key range is locked. Routing metadata still points to `shard-01`. A copy is durably recorded on `shard-02`, but physical counts have not been verified.

The timeline combines task group, move task, key-range move, and transfer journals without hiding raw states. The evidence panel separates metadata pointers, journal facts, and timestamped verification counts. The impact panel names the locked range, distribution, relations, and expected client behavior.

## Presentation mode

Presentation mode:

- hides hostnames, ports, task IDs, mutation affordances, raw errors, and unrelated navigation;
- retains the display name, selected distribution, legend, snapshot timestamp, and URL selection;
- fits common 16:9 captures;
- uses a deterministic presenter-selected light or dark theme;
- may include an optional watermark.

Direct PNG/SVG export may follow once the layout is stable; browser screenshots are sufficient for the first milestone.

## Accessibility and responsive behavior

- Make range blocks and tasks keyboard focusable; arrow keys follow range order.
- Pair every status color with text or an icon; lock patterns remain legible in grayscale.
- Show tooltip content on focus and keep important information outside tooltips.
- On narrow screens, Control Map stacks map → activity → details instead of shrinking labels.
- Preserve exact values in a screen-reader-friendly table.
