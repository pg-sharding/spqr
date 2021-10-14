#!/bin/bash

set -ex

psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=7432" -c 'show shards;' || {
    echo "ERROR: tests failed"
    exit 1
}


psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=7432" -c 'CREATE SHARDING COLUMN w_id;' || {
    echo "ERROR: tests failed"
    exit 1
}

psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=7432" -c 'ADD KEY RANGE 1 10 sh1 krid1;' || {
    echo "ERROR: tests failed"
    exit 1
}

psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=7432" -c 'ADD KEY RANGE 11 20 sh2 krid2;' || {
    echo "ERROR: tests failed"
    exit 1
}
