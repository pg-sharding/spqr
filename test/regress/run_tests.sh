#!/bin/bash

export PGDATABASE=regress
export PGUSER=regress
export PGSSLMODE=allow

run_tests () {
    DIR=$1  # router
    HOST=$2 # regress_router
    PORT=$3 # 6432

    pg_regress \
        --inputdir /regress/tests/$DIR \
        --outputdir /regress/tests/$DIR \
        --user $PGUSER \
        --dbname $PGDATABASE \
        --host $HOST \
        --port $PORT \
        --create-role $PGUSER \
        --schedule=/regress/schedule/$DIR \
        --use-existing \
        --debug || status=$?

    # show diff if it exists
    if test -f /regress/tests/$DIR/regression.diffs; then cat /regress/tests/$DIR/regression.diffs; fi
}

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
if test -f /regress/tests/console/regression.diffs; then mkdir /regress/tests/console_coordinator && mv /regress/tests/console/regression.diffs /regress/tests/console_coordinator/regression.diffs; fi
insert_greeting "console"
run_tests "console" "regress_router" "7432"
if test -f /regress/tests/console/regression.diffs; then mkdir /regress/tests/console_router && mv /regress/tests/console/regression.diffs /regress/tests/console_router/regression.diffs; fi

run_tests "router" "regress_router" "6432"
run_tests "pooler" "regress_pooler" "6432"
run_tests "coordinator" "regress_coordinator" "7002"

# these tests are to compare the results of the local and qdb coordinators
run_tests "common" "regress_coordinator" "7002"
if test -f /regress/tests/common/regression.diffs; then mkdir /regress/tests/common_coordinator && mv /regress/tests/common/regression.diffs /regress/tests/common_coordinator/regression.diffs; fi
insert_greeting "common"
run_tests "common" "regress_router" "7432"
if test -f /regress/tests/common/regression.diffs; then mkdir /regress/tests/common_router && mv /regress/tests/common/regression.diffs /regress/tests/common_router/regression.diffs; fi

# test if diffs are empty
cat /regress/tests/**/regression.diffs > /regress/tests/combined.diffs 2>&-

if test -s /regress/tests/combined.diffs; then
    exit 1
fi
exit 0
