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

psql "host=regress_router port=6432 dbname=spqr-console user=regress" -c 'ADD SHARD sh1 WITH HOSTS "shard1:6432","shard1_replica:6432" OPTIONS (dbname regress, user regress, password "12345678")'
psql "host=regress_router port=6432 dbname=spqr-console user=regress" -c 'ADD SHARD sh2 WITH HOSTS "shard2:6432","shard2_replica:6432" OPTIONS (dbname regress, user regress, password "12345678")'
psql "host=regress_router port=6432 dbname=spqr-console user=regress" -c 'ADD SHARD sh3 WITH HOSTS "shard3:6432","shard3_replica:6432" OPTIONS (dbname regress, user regress, password "12345678")'

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
