#!/bin/bash

ERR_OUTPUT_DIR=/tmp/regress_diffs

save_diffs() {
    mkdir -p $ERR_OUTPUT_DIR
    
    diff_files=$(find "$1" -name regression.diffs)
    for diff_file in ${diff_files}; do
        mv $diff_file $ERR_OUTPUT_DIR/$(basename $diff_file)

        cat $ERR_OUTPUT_DIR/$(basename $diff_file) >> /regress/tests/combined.diffs 2>&-

        cat $ERR_OUTPUT_DIR/$(basename $diff_file)
    done    
}

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

    save_diffs /regress/tests/$DIR
}
