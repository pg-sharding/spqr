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

# configure ectd

psql "host=spqr_coordinator sslmode=disable user=user1 dbname=db1 port=7002" -c 'DROP KEY RANGE ALL;' || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=spqr_coordinator sslmode=disable user=user1 dbname=db1 port=7002" -c 'DROP SHARDING COLUMN ALL;' || {
	echo "ERROR: tests failed"
	exit 1
}


psql "host=spqr_coordinator sslmode=disable user=user1 dbname=db1 port=7002" -c 'ADD KEY RANGE krid1 FROM 1 TO 10 sh1;' || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=spqr_coordinator sslmode=disable user=user1 dbname=db1 port=7002" -c 'ADD KEY RANGE krid1 FROM 11 TO 20 sh1;' || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=spqr_coordinator sslmode=disable user=user1 dbname=db1 port=7002" -c 'ADD SHARDING COLUMN id;' || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=spqr_router_1_1 port=7432 sslmode=disable user=user1 dbname=db1" -c "SHOW key_ranges;" || {
	echo "ERROR: tests failed"
	exit 1
}

