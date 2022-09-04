#!/bin/bash

export PGDATABASE=regress
export PGUSER=regress
export PGSSLMODE=disable


DIR=$1  # router
HOST=$2 # regress_router_1
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

exit $status