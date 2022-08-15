#!/bin/bash

set -ex
sleep 20

export PGDATABASE=db1
export PGUSER=user1
export PGSSLMODE=disable
export PG_REGRESS_DIFF_OPTS="-w -U3" # for alpine's diff (BusyBox)
export PATH=$PATH:"/usr/lib/postgresql/13/lib/pgxs/src/test/regress"
export PROJECT_SOURCE_DIR="/usr/local/bin/"

pg_regress --help

pg_regress \
    --inputdir $PROJECT_SOURCE_DIR \
    --outputdir $PROJECT_SOURCE_DIR \
    --user $PGUSER \
    --dbname $PGDATABASE \
    --host "spqr_router_1_1" \
    --port "7432" \
    --create-role "user1" \
    --use-existing \
    --debug console


# show diff if it exists
if test -f /usr/local/bin/regression.diffs; then cat /usr/local/bin/regression.diffs; fi

pg_regress \
    --inputdir $PROJECT_SOURCE_DIR \
    --outputdir $PROJECT_SOURCE_DIR \
    --user $PGUSER \
    --dbname $PGDATABASE \
    --host "spqr_router_1_1" \
    --port "6432" \
    --create-role "user1" \
    --use-existing \
    --debug coordinator


# show diff if it exists
if test -f /usr/local/bin/regression.diffs; then cat /usr/local/bin/regression.diffs; fi

pg_regress \
    --inputdir $PROJECT_SOURCE_DIR \
    --outputdir $PROJECT_SOURCE_DIR \
    --user $PGUSER \
    --dbname $PGDATABASE \
    --host "spqr_router_1_1" \
    --port "6432" \
    --create-role "user1" \
    --use-existing \
    --debug router

# show diff if it exists
if test -f /usr/local/bin/regression.diffs; then cat /usr/local/bin/regression.diffs; fi

exit $status