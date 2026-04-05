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
