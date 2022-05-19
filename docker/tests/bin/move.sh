#!/bin/bash

sleep 20

set -ex

psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=6432" -c 'CREATE TABLE xMove(w_id INT, s TEXT)' || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=6432" -c "insert into xMove(w_id, s) values(1, '001')" || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=6432" -c "insert into xMove(w_id, s) values(11, '002')" || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=6432" -c "select * from xMove where w_id = 11" || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=7432" -c "LOCK KEY RANGE krid2;" || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=7432" -c "MOVE KEY RANGE krid2 to sh1;" || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=7432" -c "UNLOCK KEY RANGE krid2;" || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=6432" -c "select * from xMove where w_id = 11" || {
	echo "ERROR: tests failed"
	exit 1
}

