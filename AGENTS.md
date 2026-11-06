# Rules for AI agents contributing to SPQR

Every line of a PR is read by a human maintainer. Small, focused PRs get reviewed and merged. Large or noisy ones wait.

## Hard rules

- One PR solves one issue. Link it with `Closes #N`. No issue: open one first.
- Smallest diff that solves the issue. No refactoring, renaming, reformatting or "while I'm here" fixes outside the change.
- No comments that restate code. Write a comment only for a non-obvious *why* (PostgreSQL protocol quirk, compatibility constraint). The default is no comment. SPQR SQL directives such as `/* __spqr__execute_on: sh1 */` are not comments; keep them.
- New user-visible behavior needs one short test of the cheapest type that shows it: regress (`test/regress`) by default; feature, isolation or xproto only when regress cannot reproduce it (see `spqr-tests`). Bug fixes need a test that fails without the fix.
- Unit tests only for pure logic that is hard to reach from SQL. Never duplicate with unit tests what the regress test already covers.
- Do not touch tests, golden files or expected outputs unrelated to the change.
- No `Co-Authored-By` for Claude, Codex, Copilot or any other AI tool. A human reviewer who proposed a useful change may be added as `Co-Authored-By` to the commit with that change.
- Branch name: `<category>/<kebab-case-description>`, category is `fix`, `feature`, `test`, `docs`, `refactor` or `ci`. Exception for reshke: see `spqr-pr`.
- PR description: 1–3 sentences plus `Closes #N`. No "Testing", "Summary", "Changes" or "What" sections, no file-by-file walkthrough, no list of commands you ran.

## Skills

- `spqr-pr` — title, description, commit message, scope.
- `spqr-tests` — which test to write and how.
- `spqr-self-review` — run before pushing.

## Before pushing

```bash
make fmt
make lint
make unittest
.claude/skills/spqr-self-review/scripts/check_pr.sh
```
