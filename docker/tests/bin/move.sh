#!/bin/bash

sleep 20

set -ex

psql "host=spqr_coordinator sslmode=disable user=user1 dbname=db1 port=7002" -c 'ADD SHARDING RULE r1 COLUMNS w_id;' || {
    echo "ERROR: tests failed"
    exit 1
}

psql "host=spqr_coordinator sslmode=disable user=user1 dbname=db1 port=7002" -c 'ADD KEY RANGE krid1 FROM 1 TO 10 ROUTE TO sh1;' || {
    echo "ERROR: tests failed"
    exit 1
}

psql "host=spqr_coordinator sslmode=disable user=user1 dbname=db1 port=7002" -c 'ADD KEY RANGE krid2 FROM 11 TO 20 ROUTE TO sh2;' || {
    echo "ERROR: tests failed"
    exit 1
}

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

psql "host=spqr_coordinator sslmode=disable user=user1 dbname=db1 port=7002" -c "LOCK KEY RANGE krid2;" || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=spqr_coordinator sslmode=disable user=user1 dbname=db1 port=7002" -c "MOVE KEY RANGE krid2 to sh1;" || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=spqr_coordinator sslmode=disable user=user1 dbname=db1 port=7002" -c "UNLOCK KEY RANGE krid2;" || {
	echo "ERROR: tests failed"
	exit 1
}

out=$(psql "host=spqr_shard_1 sslmode=disable user=user1 dbname=db1 port=6432" -c "select * from xMove")
test "$out" = " w_id |  s  
------+-----
    1 | 001
   11 | 002
(2 rows)"

out=$(psql "host=spqr_shard_2 sslmode=disable user=user1 dbname=db1 port=6432" -c "select * from xMove")
test "$out" = " w_id | s 
------+---
(0 rows)"

psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=6432" -c "select * from xMove where w_id = 11" || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=spqr_coordinator sslmode=disable user=user1 dbname=db1 port=7002" -c "DROP KEY RANGE ALL;" || {
	echo "ERROR: tests failed"
	exit 1
}
psql "host=spqr_coordinator sslmode=disable user=user1 dbname=db1 port=7002" -c 'DROP SHARDING RULE ALL;' || {
    echo "ERROR: tests failed"
    exit 1
}

