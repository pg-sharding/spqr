#!/bin/bash

sleep 20

set -ex

psql "host=e2e_router_1 sslmode=disable user=user1 dbname=db1 port=6432" -c 'CREATE TABLE xMove(w_id INT, s TEXT)' || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=e2e_router_1 sslmode=disable user=user1 dbname=db1 port=6432" -c "INSERT INTO xMove(w_id, s) VALUES(1, '001')" || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=e2e_router_1 sslmode=disable user=user1 dbname=db1 port=6432" -c "INSERT INTO xMove(w_id, s) VALUES(11, '002')" || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=e2e_router_1 sslmode=disable user=user1 dbname=db1 port=6432" -c "SELECT * FROM xMove WHERE w_id = 11" || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=e2e_router_1 sslmode=disable user=user1 dbname=db1 port=7432" -c "LOCK KEY RANGE krid2;" || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=e2e_router_1 sslmode=disable user=user1 dbname=db1 port=7432" -c "MOVE KEY RANGE krid2 TO sh1;" || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=e2e_router_1 sslmode=disable user=user1 dbname=db1 port=7432" -c "UNLOCK KEY RANGE krid2;" || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=e2e_router_1 sslmode=disable user=user1 dbname=db1 port=6432" -c "SELECT * FROM xMove WHERE w_id = 11" || {
	echo "ERROR: tests failed"
	exit 1
}

