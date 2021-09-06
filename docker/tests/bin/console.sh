#!/bin/bash

set -ex

psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=7432" -c 'show shards;' || {
    echo "ERROR: tests failed"
    exit 1
}



