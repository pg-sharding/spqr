---
name: spqr-self-review
description: Use before committing, pushing or opening a pull request to pg-sharding/spqr, and when asked to review or shrink a diff. Cuts the diff down to what a maintainer will actually merge.
---

# Self-review before pushing

Review your own diff as a tired maintainer would. Goal: fewer lines, one purpose.

## 1. Run the checker

```bash
.claude/skills/spqr-self-review/scripts/check_pr.sh            # compares with upstream/master, origin/master or master
.claude/skills/spqr-self-review/scripts/check_pr.sh my-base    # explicit base
```

Fix every `ERROR`. For every `WARN`, either fix it or be ready to justify it in one sentence.

## 2. Read the diff line by line

```bash
git diff $(git merge-base origin/master HEAD) --stat
git diff $(git merge-base origin/master HEAD)
```

For each hunk ask: does the issue need this line? If not, revert it.

Delete:

- comments that say what the next line does, comments that restate a function name, `// TODO` you added, doc comments on unexported helpers
- new helpers used once — inline them
- defensive checks for states that cannot happen
- new logging that nobody asked for
- reformatting, import reordering or renames in untouched code
- unit tests that duplicate the regress test (see `spqr-tests`)
- changes to unrelated tests and golden files

Keep the code in the style of the surrounding file: same error handling, same naming, same structure. Good code here needs no comments.

## 3. Final gate

- `make fmt`, `make lint`, `make unittest` pass.
- The diff is the smallest one you can defend.
- PR text follows `spqr-pr`.
