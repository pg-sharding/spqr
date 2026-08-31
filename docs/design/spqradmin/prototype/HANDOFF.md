# SPQRADMIN prototype handoff

This file records the product decisions and working context behind the current prototype. Read it together with:

- [`../../2021-spqradmin-design.md`](../../2021-spqradmin-design.md) — product, domain, gRPC, scale, and competitive research;
- [`../../2021-spqradmin-screens.md`](../../2021-spqradmin-screens.md) — required screens and behavior;
- [`../../2021-spqradmin-visual-concepts.md`](../../2021-spqradmin-visual-concepts.md) — the original visual concepts;
- [`README.md`](README.md) — local launch instructions.

## Current Git state

- Working branch: `spqradmin-prototype`.
- Latest prototype commit at the time of this handoff: `864c2b2ee` (`only one prototype for cluster and shard pages`).
- `plan.md` at the repository root is unrelated untracked user work. Do not edit, delete, or include it in SPQRADMIN commits.

## Why this feature exists

SPQR is an open-source sharding system whose important concepts are spatial: distributions divide an ordered key space into key ranges, and each range routes to a shard. Console output exposes the facts but makes topology, adjacency, ownership, and data movement difficult to understand quickly.

The UI is intended to help:

- new users understand how SPQR routes data;
- operators see what is moving, where it is moving, and where a move failed;
- operators distinguish routing metadata from transfer-journal evidence and verified physical placement;
- maintainers create clean screenshots for documentation and presentations.

This is a graphical, read-only administrative surface over Coordinator data. It is not a general PostgreSQL monitoring dashboard and is not a graphical SQL console.

## Backend and truth model

The application connects directly to the Coordinator through its existing gRPC API. A PostgreSQL-wire adapter is not part of the design.

The UI must keep these truth layers separate:

1. routing metadata — where the Coordinator currently routes a range;
2. router convergence — whether routers have received the mapping;
3. move and transfer journals — which durable phase completed;
4. physical verification — what rows actually exist on source and destination shards.

Placement claims are labeled `Known`, `Inferred`, or `Unknown`. A journal entry is not independent physical verification.

Do not invent a generic `HEALTHY` key-range status. Preserve the actual vocabularies:

- key range: `LOCKED`, `UNLOCKED` or gRPC `AVAILABLE`;
- key-range move: `PLANNED`, `AWAITED`, `LOCKED`, `DATA_MOVED`, `COORD_META_UPDATED`, `COMPLETE`;
- transfer journal: `planned`, `locked`, `data_copied`;
- move task: `PLANNED`, `SPLIT`, `MOVED`;
- task group: `PLANNED`, `RUNNING`, `ERROR`.

Selecting a key range changes focus only. It must never visually or semantically change the range to `LOCKED`. This bug existed in an early prototype and was fixed.

## Scale assumptions

The primary case is hundreds of key ranges per distribution; thousands are expected. Prototype controls cover:

- distributions: `1`, `8`, `32`;
- shards: `2`, `8`, `32`, `128`;
- key ranges per distribution: `12`, `240`, `1,842`.

The full design must also remain viable around 10,000 ranges. Dense maps therefore use a bounded overview plus an exact readable viewport rather than rendering every range as a permanently labeled card.

Shard count is independent of key-range count. Some shards may be unused in small scenarios. A move must never select the same shard as source and destination.

## Visual direction

The application should look like an operational part of [`pg-sharding.tech`](https://pg-sharding.tech/) and [`docs.pg-sharding.tech`](https://docs.pg-sharding.tech/welcome):

- light white and cool-gray surfaces;
- restrained blue accents;
- compact documentation-like navigation;
- subtle borders and shadows;
- identifiers and raw states in monospace;
- no neon NOC dashboard styling.

The logo text is always written as `SPQRADMIN`. Clicking the logo returns to the prototype root at `/`.

The user strongly prefers working visual prototypes over long speculative design prose. Keep screens simple and immediately readable. Presentation-quality screenshots are a first-class use case.

## Current information architecture

The prototype originally consisted of three complete concepts: Atlas, Transfer Desk, and Control Map. That made page-level decisions difficult to compare, so the public entry point is now organized by entity/page:

- `Cluster` — one canonical page;
- `Distribution` — three visual alternatives;
- `Range` — three visual alternatives;
- `Shard` — one canonical page;
- `Move` — three visual alternatives.

Cluster and Shard are intentionally single pages because their information architecture is straightforward. Earlier versions presented three nominal variants, but all three used the same counters, table, shard cloud, and activity list. The duplicates were removed rather than relabeled as different designs.

### Public routes

| Page | Route | Current behavior |
| --- | --- | --- |
| Start | `/` | Explains the prototype and links to all five pages |
| Cluster | `/cluster/` | Canonical cluster inventory |
| Distribution | `/distribution/` | Compares Ordered Atlas, Shard Lanes, and Map + Activity |
| Range | `/range/` | Compares Routing Facts, Move Context, and Forensic Context |
| Shard | `/shard/` | Canonical shard ownership/activity page |
| Move | `/move/` | Compares Phase Timeline, Investigation, and Data Placement |

The historical `atlas/`, `transfer-desk/`, and `control-map/` folders still implement the three alternatives for Distribution, Range, and Move. Historical Cluster and Shard URLs contain compatibility redirects to the canonical routes.

## Prototype files

- `index.html` — page-first landing page;
- `cluster/index.html` — canonical Cluster page;
- `shard/index.html` — canonical Shard page;
- `distribution/index.html`, `range/index.html`, `move/index.html` — comparison galleries;
- `demo-data.js` — deterministic scenario model and URL state;
- `entity-pages.js` — shared rendering for canonical Cluster and Shard pages;
- `gallery.css`, `gallery.js` — side-by-side comparison galleries;
- `atlas/`, `transfer-desk/`, `control-map/` — remaining page alternatives;
- `serve.py` — local static server rooted in this directory.

State is encoded in query parameters so examples remain linkable:

- `distributions` — distribution count;
- `shards` — shard count;
- `count` — key ranges per distribution;
- `distribution` — selected distribution index;
- `id` — selected `kr-N`;
- `shard` — selected `shard-N`.

## Local launch

Run from the prototype directory:

```bash
cd /Users/denchick/Code/spqr/docs/design/spqradmin/prototype
python3 serve.py
```

Then open `http://127.0.0.1:8766/` exactly as printed. A previous issue was caused by opening `http://127.0.0.1:8766/)`; the trailing `)` is part of the request path and correctly returns 404.

## Work completed

- Researched Vitess/VTAdmin, Citus/Azure shard rebalancer, and Shardman.
- Aligned the visual language with the SPQR site and documentation.
- Changed the application contract from a PostgreSQL-wire adapter to direct Coordinator gRPC.
- Separated the main design document, required screens, and visual concepts.
- Built static HTML/CSS/JS prototypes that run without package installation or a build step.
- Added variable distribution, shard, and key-range scales.
- Added cluster-wide and shard-level pages.
- Fixed selection incorrectly changing a range to `LOCKED`.
- Made every `SPQRADMIN` logo return to the prototype root.
- Reorganized the prototype from three end-to-end concepts into page-level comparisons.
- Collapsed Cluster and Shard to one canonical page each.

## Verification already performed

At the last implementation pass:

- JavaScript syntax checks passed for all shared and concept scripts;
- local targets passed for 21 HTML pages;
- the demo model passed all 36 combinations of distribution, shard, and KR scales;
- canonical Cluster and Shard routes returned HTTP 200;
- exactly one full `data-page="cluster"` page and one full `data-page="shard"` page remained;
- only Distribution, Range, and Move retained comparison galleries.

Repeat proportionate checks after further changes; do not assume this list covers future edits.

## Known limitations and open decisions

- All prototype data is simulated. No page currently calls a real Coordinator.
- No winning design has been selected for Distribution, Range, or Move.
- The original visual-concepts document still describes three end-to-end concepts. It should be updated after page-level winners are selected.
- The design document currently recommends Control Map as an integrated direction; that recommendation predates the page-first prototype structure.
- `Shard Lanes` is strongly move-oriented. Confirm whether it is a valid Distribution alternative or belongs only on a move-list page.
- Cluster and Shard currently reuse the Atlas visual foundation. Their information architecture is canonical, but their final polish may still change with the selected surrounding shell.
- Legacy Cluster and Shard HTML files use client-side redirects for saved local URLs.
- Read-only gRPC additions are still required for the two durable move journals and some timestamps described in the design document.
- No mutation controls should be added until a separate safety and authorization design is approved.

## Recommended next sequence

1. Review Distribution alternatives and select or combine the useful parts.
2. Review Range alternatives using unlocked, locked, running-move, and failed-move cases.
3. Review Move alternatives with emphasis on truth layers and safe investigation order.
4. Assemble the selected pages into one coherent navigation shell.
5. Update the visual-concepts and main design documents to match the chosen page-level design.
6. Define the exact read-only gRPC messages required by the selected screens.

## Collaboration preferences observed

- Communicate in Russian unless repository prose is intentionally English.
- Be direct about weak or duplicated concepts; do not defend cosmetic differences as meaningful alternatives.
- Prefer a working screen over another long speculative document.
- Keep explanatory copy short enough to scan.
- Preserve existing user changes and unrelated files.
