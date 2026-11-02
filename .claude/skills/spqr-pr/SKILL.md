---
name: spqr-pr
description: Use when preparing, opening or updating a pull request to pg-sharding/spqr, or when writing its title, description or commit message.
---

# Pull request to SPQR

Maintainers are few. A PR that takes more than a few minutes to understand is postponed.

## 1. Check scope before writing code

- There is an issue. If not, stop and open one; describe the problem, not the solution.
- The PR changes only what the issue asks. Unrelated cleanup goes to a separate PR, or nowhere.
- If the change touches more than one subsystem (router, coordinator, qdb, balancer), ask whether it can be split into sequential PRs. Usually it can: storage first, user-visible part second.

## 2. Title

One short sentence, imperative, no prefix, no trailing period. Match history (`git log --oneline -20`):

- Good: `Show key range timestamps in SHOW key_ranges_extended`
- Bad: `feat(qdb): store key range creation time and expose both timestamps in SHOW key_ranges_extended`

## 3. Description

Exactly this shape, nothing else:

```
<What changes and why, 1–3 sentences. User-visible effect first.>

Closes #<issue>
```

Example:

```
Add created_at and updated_at columns to SHOW key_ranges_extended. Ranges created before the upgrade show NULL.

Closes #3134
```

Never add:

- `## Testing`, `## Summary`, `## What`, `## Changes`, `## Flow` headings
- lists of functions or files you changed — the diff shows them
- commands you ran, "all tests pass", "build is clean"
- excuses about what you could not run locally
- the same text repeated in the commit message body

If a reviewer must know a non-obvious decision (compatibility, migration), add one sentence for it.

## 4. Commit

One commit per PR unless the maintainer asks otherwise. Subject = PR title. Body optional, at most 2–3 lines, only for the *why*.

## 5. Review feedback

- Fix the code; push. Reply only when you disagree or need a decision, in one or two sentences.
- Bot reviews (CodeRabbit etc.): apply what is correct, ignore nitpicks that grow the diff.
- Do not rewrite the description into a changelog of review rounds.
