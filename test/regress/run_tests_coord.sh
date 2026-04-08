#!/bin/bash
set -x 

export PGDATABASE=regress
export PGUSER=regress
export PGSSLMODE=allow

source ./regress_utils.sh 

echo "wait for services started"
sleep 10
echo "init cluster"
run_tests "init_cluster" "regress_coordinator" "7002"
sleep 10
echo "go test!"

run_tests "console" "regress_coordinator" "7002"

run_tests "console" "regress_router" "7432"


run_tests "router" "regress_router" "6432"
run_tests "pooler" "regress_pooler" "6432"

# the next test uses a "registering router r1" 
run_tests "kill_cluster" "regress_coordinator" "7002"
sleep 10
run_tests "coordinator" "regress_coordinator" "7002"

echo "init cluster"
run_tests "init_cluster" "regress_coordinator" "7002"
sleep 10
echo "go test!"
# Compare the results of the local and qdb coordinators
run_tests "common" "regress_coordinator" "7002"

run_tests "common" "regress_router" "7432"

# test if diffs are empty
cat $ERR_OUTPUT_DIR/regression.diffs > /regress/tests/combined.diffs 2>&-

if test -s /regress/tests/combined.diffs; then
    exit 1
fi
exit 0
