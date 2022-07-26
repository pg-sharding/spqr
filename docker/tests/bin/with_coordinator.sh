#!/bin/bash

sleep 20

set -ex

psql "host=spqr_coordinator sslmode=disable user=user1 dbname=db1 port=7002" -c 'REGISTER ROUTER spqr_router_1_1:7000 r1' || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=spqr_coordinator sslmode=disable user=user1 dbname=db1 port=7002" -c 'add key range 1 10 sh1 krid1' || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=spqr_coordinator sslmode=disable user=user1 dbname=db1 port=7002" -c 'add key range 11 20 sh2 krid2' || {
	echo "ERROR: tests failed"
	exit 1
}
