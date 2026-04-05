#!/bin/bash

export PGDATABASE=regress
export PGUSER=regress
export PGSSLMODE=allow

source ./regress_utils.sh 

insert_greeting () {
    testDir=$1
    for f in /regress/tests/$testDir/expected/*; do 
    echo -e "
\t\tSPQR router admin console
\tHere you can configure your routing rules
------------------------------------------------
\tYou can find documentation here 
https://github.com/pg-sharding/spqr/tree/master/docs
" > tmpfile
        cat $f >> tmpfile
        mv tmpfile $f
    done
}


run_tests "console" "regress_coordinator" "7002"

insert_greeting "console"

run_tests "console" "regress_router" "7432"

run_tests "router" "regress_router" "6432"

run_tests "pooler" "regress_pooler" "6432"

run_tests "coordinator" "regress_coordinator" "7002"
save_diffs /regress/tests/coordinator/
# these tests are to compare the results of the local and qdb coordinators
run_tests "common" "regress_coordinator" "7002"

insert_greeting "common"
run_tests "common" "regress_router" "7432"

# test if diffs are empty

if test -s /regress/tests/combined.diffs; then
    exit 1
fi
exit 0
