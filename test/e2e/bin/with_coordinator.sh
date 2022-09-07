#!/bin/bash

sleep 20

set -ex

psql "host=e2e_router_1 port=7432 sslmode=disable" -c "SHOW key_ranges;"|| {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=e2e_coordinator sslmode=disable port=7002" -c "REGISTER ROUTER r1 ADDRESS e2e_router_1:7000;" || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=e2e_router_1 port=7432 sslmode=disable user=user1 dbname=db1" -c "SHOW key_ranges;"|| {
	echo "ERROR: tests failed"
	exit 1
}

# configure ectd

psql "host=e2e_coordinator sslmode=disable user=user1 dbname=db1 port=7002" -c 'DROP KEY RANGE ALL;' || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=e2e_coordinator sslmode=disable user=user1 dbname=db1 port=7002" -c 'DROP SHARDING RULE ALL;' || {
	echo "ERROR: tests failed"
	exit 1
}


psql "host=e2e_coordinator sslmode=disable user=user1 dbname=db1 port=7002" -c 'ADD KEY RANGE krid1 FROM 1 TO 10 ROUTE TO sh1;' || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=e2e_coordinator sslmode=disable user=user1 dbname=db1 port=7002" -c 'ADD KEY RANGE krid2 FROM 11 TO 20 ROUTE TO sh1;' || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=e2e_coordinator sslmode=disable user=user1 dbname=db1 port=7002" -c 'ADD SHARDING RULE rule1 COLUMNS id;' || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=e2e_router_1 port=7432 sslmode=disable user=user1 dbname=db1" -c "SHOW key_ranges;" || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=e2e_router_1 port=7432 sslmode=disable user=user1 dbname=db1" -c "SHOW sharding_rules;" || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=e2e_coordinator sslmode=disable user=user1 dbname=db1 port=7002" -c 'DROP SHARDING RULE rule1;' || {
	echo "ERROR: tests failed"
	exit 1
}

