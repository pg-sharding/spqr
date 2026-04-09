#!/bin/bash
set -x 

export PGDATABASE=regress
export PGUSER=regress
export PGSSLMODE=allow

source ./regress_utils.sh 

run_tests "console" "regress_coordinator" "7002"

run_tests "console" "regress_router" "7432"

run_tests "router" "regress_router" "6432"

run_tests "pooler" "regress_pooler" "6432"

run_tests "coordinator" "regress_coordinator" "7002"

# these tests are to compare the results of the local and qdb coordinators
run_tests "common" "regress_coordinator" "7002"

run_tests "common" "regress_router" "7432"

# test if diffs are empty

if test -s /regress/tests/combined.diffs; then
    exit 1
fi
exit 0
