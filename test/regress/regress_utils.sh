#!/bin/bash
set -x 

ERR_OUTPUT_DIR=/tmp/regress_diffs
REGRESS_REPORT_DIR=${REGRESS_REPORT_DIR:-/regress/test-reports/regress}
RUN_TESTS_SEQ=0

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
    RUN_TESTS_SEQ=$((RUN_TESTS_SEQ + 1))

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

    ./pg_regress_to_junit \
        --suite "$DIR-$HOST-$PORT" \
        --regression-out "/regress/tests/$DIR/regression.out" \
        --diffs "/regress/tests/$DIR/regression.diffs" \
        --output "$REGRESS_REPORT_DIR/$(printf "%02d" "$RUN_TESTS_SEQ")-$DIR-$HOST-$PORT.xml"

    save_diffs /regress/tests/$DIR
}
