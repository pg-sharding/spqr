#!/usr/bin/env bash
# Usage: check_pr.sh [base-ref]
# Optional: PR_BODY_FILE=path/to/description.md check_pr.sh
set -uo pipefail

base_ref="${1:-}"
if [ -z "$base_ref" ]; then
  for cand in upstream/master origin/master master; do
    if git rev-parse --verify --quiet "${cand}^{commit}" >/dev/null; then
      base_ref="$cand"
      break
    fi
  done
fi
if [ -z "$base_ref" ]; then
  echo "ERROR: base branch not found, pass it as the first argument" >&2
  exit 2
fi

base=$(git merge-base "$base_ref" HEAD)
errors=0
warns=0
err() { echo "ERROR: $*"; errors=$((errors + 1)); }
warn() { echo "WARN:  $*"; warns=$((warns + 1)); }

excludes=(
  ':(glob,exclude)vendor/**'
  ':(glob,exclude)**/go.sum'
  ':(glob,exclude)**/*.pb.go'
  ':(glob,exclude)**/mock/**'
  ':(glob,exclude)yacc/**/gram.go'
  ':(glob,exclude)test/regress/tests/*/expected/*.out'
)

sum_added() { awk '$1 != "-" {a += $1} END {print a + 0}'; }

echo "Base: $base_ref ($(git rev-parse --short "$base"))"

if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
  warn "uncommitted changes are not checked, commit them first"
fi

read -r added deleted < <(git diff --numstat "$base" HEAD -- . "${excludes[@]}" |
  awk '$1 != "-" {a += $1; d += $2} END {print a + 0, d + 0}')
files=$(git diff --name-only "$base" HEAD -- . "${excludes[@]}" | wc -l | tr -d ' ')
echo "Diff: +$added -$deleted in $files files (generated code and expected outputs excluded)"

changed=$((added + deleted))
if [ "$changed" -gt 600 ]; then
  err "diff is $changed lines; split the PR"
elif [ "$changed" -gt 300 ]; then
  warn "diff is $changed lines; can it be split or shrunk?"
fi
if [ "$files" -gt 10 ]; then
  warn "$files files touched; check every file is needed"
fi

test_added=$(git diff --numstat "$base" HEAD -- ':(glob)**/*_test.go' | sum_added)
prod_added=$(git diff --numstat "$base" HEAD -- ':(glob)**/*.go' ':(glob,exclude)**/*_test.go' "${excludes[@]}" | sum_added)
if [ "$test_added" -gt 40 ] && [ "$test_added" -gt "$prod_added" ]; then
  warn "unit tests add $test_added lines vs $prod_added lines of code; keep unit tests only for pure logic"
fi

comments=$(git diff -U0 "$base" HEAD -- ':(glob)**/*.go' "${excludes[@]}" | awk '
  /^\+\+\+ / { file = substr($0, 7); next }
  /^@@/ { split($3, a, ","); line = substr(a[1], 2) + 0; next }
  /^\+/ {
    text = substr($0, 2)
    if (text ~ /^[[:space:]]*\/\// && text !~ /^[[:space:]]*\/\/(go:|nolint|lint:)/) {
      print "  " file ":" line ": " text
    }
    line++
  }
')
if [ -n "$comments" ]; then
  warn "added comments; keep only a non-obvious why:"
  echo "$comments"
fi

regress_changed=$(git diff --name-only "$base" HEAD -- test/regress test/feature/features test/isolation test/xproto test/drivers | wc -l | tr -d ' ')
if [ "$prod_added" -gt 0 ] && [ "$regress_changed" -eq 0 ]; then
  warn "Go code changed but no regress, feature, isolation, xproto or driver test; features and bug fixes need one"
fi

while IFS= read -r sql; do
  [ -z "$sql" ] && continue
  suite=$(echo "$sql" | cut -d/ -f4)
  name=$(basename "$sql" .sql)
  expected="test/regress/tests/$suite/expected/$name.out"
  if ! git cat-file -e "HEAD:$expected" 2>/dev/null; then
    err "$sql has no $expected"
  elif git diff --quiet "$base" HEAD -- "$expected"; then
    warn "$sql changed but $expected did not"
  fi
  if ! git show "HEAD:test/regress/schedule/$suite" 2>/dev/null | grep -qx "test: $name"; then
    err "$name is not listed in test/regress/schedule/$suite"
  fi
  sql_lines=$(git diff --numstat "$base" HEAD -- "$sql" | sum_added)
  if [ "$sql_lines" -gt 40 ]; then
    warn "$sql adds $sql_lines lines; regress tests should be short"
  fi
done < <(git diff --name-only --diff-filter=AM "$base" HEAD -- ':(glob)test/regress/tests/*/sql/*.sql')

commits=$(git rev-list --count "$base"..HEAD)
if [ "$commits" -gt 1 ]; then
  warn "$commits commits; squash into one"
fi
subject=$(git log -1 --format=%s)
if echo "$subject" | grep -Eq '^[a-z]+(\([^)]*\))?!?: '; then
  warn "commit subject uses a conventional-commit prefix; repo history does not: \"$subject\""
fi
if [ "${#subject}" -gt 72 ]; then
  warn "commit subject is ${#subject} chars; keep it under 72"
fi
body_lines=$(git log -1 --format=%b | grep -v '^[[:space:]]*$' | grep -cvE '^[A-Za-z-]+: ')
if [ "$body_lines" -gt 4 ]; then
  warn "commit body has $body_lines lines; keep only the why, 2-3 lines"
fi

if [ -n "${PR_BODY_FILE:-}" ]; then
  if [ ! -f "$PR_BODY_FILE" ]; then
    err "PR_BODY_FILE $PR_BODY_FILE not found"
  else
    if ! grep -Eqi '(closes|fixes|resolves) +(([a-z0-9_.-]+/)?[a-z0-9_.-]+)?#[0-9]+' "$PR_BODY_FILE"; then
      err "PR description has no 'Closes #N'"
    fi
    if grep -Eqi '^#+[[:space:]]*(testing|tests|test plan)' "$PR_BODY_FILE"; then
      err "PR description has a Testing section; remove it"
    fi
    if grep -Eq '^#+[[:space:]]' "$PR_BODY_FILE"; then
      warn "PR description has headings; use 1-3 plain sentences"
    fi
    body_text_lines=$(grep -cv '^[[:space:]]*$' "$PR_BODY_FILE")
    if [ "$body_text_lines" -gt 5 ]; then
      warn "PR description has $body_text_lines non-empty lines; aim for 1-3 sentences plus Closes #N"
    fi
  fi
fi

echo "Result: $errors error(s), $warns warning(s)"
[ "$errors" -eq 0 ]
