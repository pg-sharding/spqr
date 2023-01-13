#!/bin/bash

set -x

export PGDATABASE=regress
export PGUSER=regress
export PGSSLMODE=disable

<<<<<<< Updated upstream

DIR=$1  # router
HOST=$2 # regress_router_1
PORT=$3 # 6432
=======
run_tests () {
    DIR=$1  # router
    HOST=$2 # regress_router
    PORT=$3 # 6432

    timeout 1m pg_regress \
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
>>>>>>> Stashed changes


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

<<<<<<< Updated upstream
# show diff if it exists
if test -f /regress/tests/$DIR/regression.diffs; then cat /regress/tests/$DIR/regression.diffs; fi

exit $status
=======
if test -f /regress/regress.diffs; then
    cat /regress/regress.diffs
    exit 1
fi

exit 0
>>>>>>> Stashed changes
