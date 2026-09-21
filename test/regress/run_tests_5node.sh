#!/bin/bash
set -x

export PGDATABASE=regress
export PGUSER=regress
export PGHOST=127.0.0.1
export PGPORT=6432
export PGSSLMODE=allow

source ./regress_utils.sh

run_tests "5node" "127.0.0.1" "6432"

# test if diffs are empty

if test -s /regress/tests/combined.diffs; then
    exit 1
fi
exit 0