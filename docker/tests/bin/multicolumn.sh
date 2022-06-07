#!/bin/bash

sleep 20

set -ex

psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=6432" -c 'DROP TABLE IF EXISTS x_x_x' || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=6432" -c 'CREATE TABLE x_x_x(w_id INT, s TEXT)' || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=6432" -c 'DROP TABLE IF EXISTS x_x' || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=6432" -c 'CREATE TABLE x_x(w_id INT, b_id INT)' || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=6432" -c 'insert into x_x (w_id, b_id) values(1, 2), (2, 3), (3, 4)' || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=6432" -c 'insert into x_x (w_id, b_id) values(11, 12), (12, 13), (13, 14)' || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=6432" -c 'select * from x_x where w_id >= 11' || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=6432" -c 'select * from x_x where w_id <= 10' || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=6432" -c 'insert into x_x_x (w_id, s) values(1, '2'), (2, '3'), (3, '4')' || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=6432" -c 'insert into x_x_x (w_id, s) values(11, '12'), (12, '13'), (13, '14')' || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=6432" -c 'select * from x_x_x where w_id >= 11' || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=6432" -c 'select * from x_x_x where w_id <= 10' || {
	echo "ERROR: tests failed"
	exit 1
}

