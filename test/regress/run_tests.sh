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
    for f in /regress/tests/common/expected/*; do 
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


run_tests "console" "regress_router" "7432"
run_tests "router" "regress_router" "6432"
run_tests "pooler" "regress_pooler" "6432"
run_tests "coordinator" "regress_coordinator" "7002"

run_tests "common" "regress_coordinator" "7002"
insert_greeting
run_tests "common" "regress_router" "7432"

# test if diffs are empty
cat /regress/tests/**/regression.diffs > /regress/tests/combined.diffs 2>&-

if test -s /regress/tests/combined.diffs; then
    sleep 3000000s
    exit 1
fi
exit 0
