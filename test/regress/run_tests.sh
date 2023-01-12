#!/bin/bash

export PGDATABASE=regress
export PGUSER=regress
export PGSSLMODE=disable

STATUS=0

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

    if [ $status -ne 0 ]; then
        STATUS=$status
    fi
}


sleep 10
run_tests "router" "regress_router" "6432"
run_tests "console" "regress_router" "7432"
run_tests "coordinator" "regress_coordinator" "7002"

exit $STATUS

