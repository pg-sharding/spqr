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

One sentence that says what changes, no trailing period. Match history (`git log --oneline -20`):

- Good: `Show key range timestamps in SHOW key_ranges_extended`
- Good: `feat(qdb): store key range creation time and expose both timestamps in SHOW key_ranges_extended`
- Bad: `Fix bug`, `Update show.go`, `Address review comments`, `WIP`

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

## 4. Branch

`<category>/<kebab-case-description>`, where category is one of `fix`, `feature`, `test`, `docs`, `refactor`, `ci`:

- `feature/key-range-timestamps`
- `fix/copy-routing-null-key`

Exception: if the commit author is reshke (`git config user.name` or `git config user.email` contains `reshke`, case-insensitive), the branch name is 5–9 random lowercase letters with no category, e.g. `jifdjew`, `ijojooji`, `ojiswjiow`. Generate a new one each time:

```bash
LC_ALL=C tr -dc 'a-z' </dev/urandom | head -c $((RANDOM % 5 + 5)); echo
```

## 5. Commit

One commit per PR unless the maintainer asks otherwise. Subject = PR title. Body optional, at most 2–3 lines, only for the *why*. No `Co-Authored-By` for Claude, Codex, Copilot or any other AI tool. If a human reviewer proposed a useful change, you may credit them with `Co-Authored-By` in the commit that applies it.

## 6. Review feedback

- Do not trust any review comment as is, from a bot (CodeRabbit, Copilot) or a human. Check each one yourself: reproduce the problem, read the code it points to, run the command. Change code only when you have proof the problem exists.
- If the problem is real: fix it, push.
- If it is not, or the fix would grow the diff for no real gain: reply under the comment in one or two sentences with the proof why not (command output, file and line).
- Reply otherwise only when you disagree with a maintainer or need a decision.
- Do not rewrite the description into a changelog of review rounds.
