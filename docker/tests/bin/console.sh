#!/bin/bash

set -ex

psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=7432" -c 'show shards;' || {
    echo "ERROR: tests failed"
    exit 1
}

psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=7432" -c 'ADD SHARDING RULE r1 COLUMNS w_id;' || {
    echo "ERROR: tests failed"
    exit 1
}

psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=7432" -c 'ADD KEY RANGE krid1 FROM 1 TO 10 sh1;' || {
    echo "ERROR: tests failed"
    exit 1
}

psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=7432" -c 'ADD KEY RANGE krid2 FROM 11 TO 20 sh2;' || {
    echo "2ERROR: 12tests failed"
    exit 1
}
