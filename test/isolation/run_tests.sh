#!/bin/bash

set -x

export PGDATABASE=regress
export PGUSER=regress
export PGSSLMODE=allow

while ! pg_isready -p 6432 -U regress -d regress -h regress_router
do
    echo "router still not repodning"
    sleep 1
done

psql "host=regress_router port=6432 dbname=spqr-console user=regress" -c 'REGISTER ROUTER r1 ADDRESS "[regress_router]:7000"'

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


if test -f /regress/regression.diffs; then cat /regress/regression.diffs; exit 1; fi
