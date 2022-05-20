#!/bin/bash

sleep 20

set -ex

psql "host=spqr_router_1_1 port=7432 sslmode=disable" -c "SHOW key_ranges;"|| {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=spqr_coordinator sslmode=disable port=7002" -c "REGISTER ROUTER spqr_router_1_1:7000 r1;" || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=spqr_router_1_1 port=7432 sslmode=disable user=user1 dbname=db1" -c "SHOW key_ranges;"|| {
	echo "ERROR: tests failed"
	exit 1
}

