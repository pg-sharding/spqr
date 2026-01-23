#!/bin/bash

export PGDATABASE=regress
export PGUSER=regress
export PGSSLMODE=allow

/postgres/src/test/isolation/pg_isolation_regress \
    --inputdir /regress \
    --outputdir /regress \
    --user regress \
    --dbname regress \
    --host regress_router \
    --port 6432 \
    --create-role regress \
    --use-existing \
    --debug \
    --schedule /regress/schedule

if test -f /regress/regression.diffs; then cat /regress/regression.diffs; fi

