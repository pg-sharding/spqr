#!/bin/bash

sleep 100

psql "host=spqr_router_1 sslmode=disable user=user1 password=password dbname=db1 port=6432" -c 'insert into x values(1)' || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=spqr_router_1 sslmode=disable user=user1 password=password dbname=db1 port=6432" -c 'insert into x values(11)' || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=spqr_router_1 sslmode=disable user=user1 password=password dbname=db1 port=6432" -c 'select from x where w_id = 11' || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=spqr_router_1 sslmode=disable user=user1 password=password dbname=db1 port=6432" -c 'select from x where w_id = 1' || {
	echo "ERROR: tests failed"
	exit 1
}
