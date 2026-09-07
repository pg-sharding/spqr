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

# Derives schedule $2 from schedule $1 by dropping the test names listed in the
# file $3 (one name per line, # comments allowed). Variants of the suite stay in
# sync with the base schedule that way: a newly added test is picked up unless
# it is explicitly skipped.
build_schedule () {
    BASE=/regress/schedule/$1
    OUT=/regress/schedule/$2
    SKIP_FILE=$3

    cp $BASE $OUT

    while read -r test_name; do
        case "$test_name" in
            ''|\#*) continue ;;
        esac
        sed -i "/^test: ${test_name}\$/d" $OUT
    done < $SKIP_FILE
}

run_tests () {
    DIR=$1       # router
    HOST=$2      # regress_router
    PORT=$3      # 6432
    SCHEDULE=${4:-$DIR} # schedule file name, defaults to the tests dir name
    RUN_TESTS_SEQ=$((RUN_TESTS_SEQ + 1))

    pg_regress \
        --inputdir /regress/tests/$DIR \
        --outputdir /regress/tests/$DIR \
        --user $PGUSER \
        --dbname $PGDATABASE \
        --host $HOST \
        --port $PORT \
        --create-role $PGUSER \
        --schedule=/regress/schedule/$SCHEDULE \
        --use-existing \
        --debug || status=$?

    ./pg_regress_to_junit \
        --suite "$SCHEDULE-$HOST-$PORT" \
        --regression-out "/regress/tests/$DIR/regression.out" \
        --diffs "/regress/tests/$DIR/regression.diffs" \
        --output "$REGRESS_REPORT_DIR/$(printf "%02d" "$RUN_TESTS_SEQ")-$SCHEDULE-$HOST-$PORT.xml"

    save_diffs /regress/tests/$DIR
}
