#!/bin/bash

set -x

export PGDATABASE=regress
export PGUSER=regress
export PGSSLMODE=allow

while ! pg_isready -p 6432 -U regress -d regress -h regress_router
do
    echo "router still not responding"
    sleep 1
done

psql "host=regress_coordinator port=7002 dbname=regress user=regress" -c 'CREATE SHARD sh1 OPTIONS (host "spqr_shard_1:6432", host "spqr_shard_1_replica:6432", dbname regress, user regress, password "12345678")'
psql "host=regress_coordinator port=7002 dbname=regress user=regress" -c 'CREATE SHARD sh2 OPTIONS (host "spqr_shard_2:6432", host "spqr_shard_2_replica:6432", dbname regress, user regress, password "12345678")'
psql "host=regress_coordinator port=7002 dbname=regress user=regress" -c 'CREATE SHARD sh3 OPTIONS (host "spqr_shard_3:6432", host "spqr_shard_3_replica:6432", dbname regress, user regress, password "12345678")'
psql "host=regress_coordinator port=7002 dbname=regress user=regress" -c 'CREATE SHARD sh4 OPTIONS (host "spqr_shard_4:6432", host "spqr_shard_4_replica:6432", dbname regress, user regress, password "12345678")'

psql "host=regress_router port=6432 dbname=spqr-console user=regress" -c 'REGISTER ROUTER r1 ADDRESS "[regress_router]:7000"'
psql "host=regress_router port=6432 dbname=spqr-console user=regress" -c 'REGISTER ROUTER r2 ADDRESS "[regress_router_2]:7000"'

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

if test -f /regress/regression.diffs;
then
    cat /regress/regression.diffs; exit 1;
fi
