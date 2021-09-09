#!/bin/bash

set -ex

psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=6432" -c 'select * from x where w_id = 111' || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=6432" -c 'select * from x where w_id = 101' || {
	echo "ERROR: tests failed"
	exit 1
}