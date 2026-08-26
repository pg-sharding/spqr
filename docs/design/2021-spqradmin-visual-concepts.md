# SPQR Admin: visual concepts

| Field | Value |
| --- | --- |
| Status | Exploration — visual concept decision pending |
| Parent design | [SPQR Admin design](2021-spqradmin-design.md) |
| Screen requirements | [Required screens and behavior](2021-spqradmin-screens.md) |
| Related issue | [pg-sharding/spqr#2021](https://github.com/pg-sharding/spqr/issues/2021) |
| Last updated | 2026-08-26 |

All concepts use the same SPQR visual language, exact state vocabulary, and semantic-zoom model defined in the parent design. They differ only in which question dominates the first screen.

## A — Atlas

The ordered key space is the main object; shard-colored ranges and movement arrows optimize for teaching and screenshots.

```text
Distribution: customers_by_id       842 ranges · Ordered ranges

Full distribution overview
┌──────────────────────────────────────────────────────────────────────┐
│ sh1 ░░░░░░ sh2 ▒▒▒▒▒▒▒▒▒▒ sh3 ▓▓▓▓▓▓▓ ▲LOCKED ▓▓▓▓ sh1 ░░░░░░░ │
└───────────────────────[ selected window ]────────────────────────────┘

Ranges 181–204 of 842
┌────────┬────────┬────────┬────────┬───────┬───────────────┐
│ kr-181 │ kr-182 │ kr-183 │ kr-184 │  ···  │ kr-204        │
│ sh1    │ sh1    │ sh2    │ sh2    │       │ sh3 · LOCKED │
└────────┴────────┴────────┴────────┴───────┴───────────────┘

Selected: kr-204 · [200k, 300k) · LOCKED · task group ERROR
```

## B — Transfer Desk

Shard columns and source-to-destination task cards optimize for movement and recovery.

```text
shard-01                       shard-02                       shard-03
┌────────────────┐            ┌────────────────┐             ┌────────────────┐
│ 418 ranges     │ kr-204 ──▶ │ 287 ranges     │             │ 137 ranges     │
│ 1 LOCKED       │            │ task ERROR     │             │ 0 LOCKED       │
└────────────────┘            └────────────────┘             └────────────────┘

Task tg-7f31
✓ Prepare  ✓ Lock  ✓ Copy  ✕ Delete source  ○ Update metadata  ○ Unlock

Raw: KR LOCKED · move LOCKED · transfer data_copied · task group ERROR
Routing metadata: shard-01        Physical: source known; target copy recorded
Last error: DELETE FROM public.orders: connection reset
```

## C — Control Map

An Atlas-style map shares selection with a persistent activity rail and evidence drawer.

```text
Distribution: customers_by_id                        3 shards · 842 ranges
┌──────────────────────────────────────────────────────────────────────┐
│ Full overview  sh1 ░░░░░░░░ sh2 ▒▒▒▒ ▲ERROR ▒▒ sh3 ▓▓▓▓▓▓▓▓▓▓ │
│ Window 181–204 [kr-181][kr-182][kr-183] ··· [kr-204 LOCKED]        │
└──────────────────────────────────────────────────────────────────────┘

Activity
┌──────────┬────────────────────┬──────────┬──────────┬────────────────┐
│ ERROR    │ kr-204 · sh1 → sh2│ delete   │ LOCKED   │ connection ... │
│ RUNNING  │ kr-611 · sh3 → sh2│ Copy 2/5 │ 8s ago   │                │
└──────────┴────────────────────┴──────────┴──────────┴────────────────┘
```

## Comparison and recommendation

| Criterion | A — Atlas | B — Transfer Desk | C — Control Map |
| --- | --- | --- | --- |
| Product promise | Understand SPQR in one screen | Recover moves safely | See where data belongs and what is happening to it |
| New-user comprehension | Excellent | Limited | Excellent |
| Move/failure diagnosis | Good / fair | Excellent | Excellent |
| Presentation screenshots | Excellent | Good | Very good |
| Large topology scanning | Very good | Good | Very good |
| Main strength | Adjacency, ownership, split/unite | Phase and evidence model | Links failure to its exact interval |
| Main trade-off | Failure timeline needs a drawer | Topology becomes secondary | Highest responsive and disclosure effort |
| Implementation scope | Medium | Medium–high | High |
| Clutter risk | Low | Medium | Medium–high |

Choose **C — Control Map** as the target direction, staged as Atlas plus a read-only activity rail. If the team needs the smallest independent release, ship **A — Atlas** and use Transfer Desk as task detail rather than another application shell.
