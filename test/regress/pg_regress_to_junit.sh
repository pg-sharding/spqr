#!/bin/bash

set -u

suite=""
regression_out=""
diffs=""
output=""

while [ "$#" -gt 0 ]; do
    case "$1" in
        --suite)
            suite="$2"
            shift 2
            ;;
        --regression-out)
            regression_out="$2"
            shift 2
            ;;
        --diffs)
            diffs="$2"
            shift 2
            ;;
        --output)
            output="$2"
            shift 2
            ;;
        *)
            echo "unknown argument: $1" >&2
            exit 2
            ;;
    esac
done

if [ -z "$suite" ] || [ -z "$regression_out" ] || [ -z "$output" ]; then
    echo "usage: $0 --suite NAME --regression-out FILE [--diffs FILE] --output FILE" >&2
    exit 2
fi

mkdir -p "$(dirname "$output")"
if [ ! -f "$regression_out" ]; then
    regression_out=/dev/null
fi
if [ -n "$diffs" ] && [ ! -f "$diffs" ]; then
    diffs=/dev/null
fi

awk -v suite="$suite" -v diffs_file="$diffs" '
function esc(s) {
    gsub(/&/, "\\&amp;", s)
    gsub(/</, "\\&lt;", s)
    gsub(/>/, "\\&gt;", s)
    gsub(/"/, "\\&quot;", s)
    gsub(/\047/, "\\&apos;", s)
    return s
}

function add_result(name, result) {
    if (name == "") {
        return
    }
    if (!(name in seen)) {
        order[++count] = name
        seen[name] = 1
    }
    if (result == "failed" || status[name] == "") {
        status[name] = result
    }
}

BEGIN {
    while (diffs_file != "" && (getline line < diffs_file) > 0) {
        diff_text = diff_text line "\n"
        if (line ~ /(expected|results)\/[^\/]+\.out/) {
            name = line
            sub(/^.*\/(expected|results)\//, "", name)
            sub(/\.out.*$/, "", name)
            current_diff_name = name
            add_result(name, "failed")
        }
        if (current_diff_name != "") {
            diff_by_test[current_diff_name] = diff_by_test[current_diff_name] line "\n"
        }
    }
    if (diffs_file != "") {
        close(diffs_file)
    }
}

/^ok[[:space:]]+[0-9]+[[:space:]]+-[[:space:]]+/ {
    name = $0
    sub(/^ok[[:space:]]+[0-9]+[[:space:]]+-[[:space:]]+/, "", name)
    add_result(name, "passed")
    next
}

/^not ok[[:space:]]+[0-9]+[[:space:]]+-[[:space:]]+/ {
    name = $0
    sub(/^not ok[[:space:]]+[0-9]+[[:space:]]+-[[:space:]]+/, "", name)
    add_result(name, "failed")
    next
}

/^test[[:space:]].*[[:space:]]\.\.\.[[:space:]]+ok/ {
    name = $0
    sub(/^test[[:space:]]+/, "", name)
    sub(/[[:space:]]+\.\.\.[[:space:]]+ok.*$/, "", name)
    add_result(name, "passed")
    next
}

/^test[[:space:]].*[[:space:]]\.\.\.[[:space:]]+(FAILED|failed)/ {
    name = $0
    sub(/^test[[:space:]]+/, "", name)
    sub(/[[:space:]]+\.\.\.[[:space:]]+(FAILED|failed).*$/, "", name)
    add_result(name, "failed")
    next
}

END {
    failures = 0
    for (i = 1; i <= count; i++) {
        if (status[order[i]] == "failed") {
            failures++
        }
    }
    if (count == 0) {
        order[++count] = suite
        status[suite] = (diff_text == "" ? "passed" : "failed")
        failures = (diff_text == "" ? 0 : 1)
    }

    print "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
    printf "<testsuite name=\"%s\" tests=\"%d\" failures=\"%d\" errors=\"0\" skipped=\"0\">\n", esc(suite), count, failures
    for (i = 1; i <= count; i++) {
        name = order[i]
        printf "  <testcase classname=\"%s\" name=\"%s\">", esc(suite), esc(name)
        if (status[name] == "failed") {
            failure_text = diff_by_test[name]
            if (failure_text == "") {
                failure_text = diff_text
            }
            printf "\n    <failure message=\"regression diff\">%s</failure>\n  ", esc(failure_text)
        }
        print "</testcase>"
    }
    print "</testsuite>"
}
' "$regression_out" > "$output"
