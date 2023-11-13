#!/bin/bash

sleep 20

set -ex

function clearID() {
    sed -E 's/0x[0-9a-f]+/************/g'
}

function clearStatistics() {
    sed -E 's/[0-9]+[.][0-9]+ms/*****/g' | 
    sed -E 's/_[0-9]+[.][0-9]/***/g'
}

function clearTableFormating() {
    sed -E 's/[+][-]+/+-/g' |
    sed -E 's/[*][ ]+[|]/* |/g'
}

out=$(psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=7432" -c 'show clients;' | clearID | clearStatistics)
test "$out" = " client_id | user | dbname | server_id | router_address | router_time*** | shard_time*** 
-----------+------+--------+-----------+----------------+-----------------+----------------
(0 rows)"

psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=6432" <<EOH &
select 1;
SELECT pg_sleep(20);
select 1;
EOH

sleep 10

out=$(psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=7432" -c 'show clients;' | clearID | clearStatistics | clearTableFormating)
test "$out" = "  client_id   | user  | dbname |     server_id     | router_address | router_time*** | shard_time*** 
--------------+-+-+-+-+-+-
 ************ | user1 | db1    | spqr_shard_1:6432 | local          | ***** | *****
 ************ | user1 | db1    | spqr_shard_2:6432 | local          | ***** | *****
(2 rows)" || {
    echo "no where should work"
    exit 1
}

out=$(psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=7432" -c 'show clients where server_id = spqr_shard_1:6432 or dbname = db1;' | clearID | clearStatistics | clearTableFormating)
test "$out" = "  client_id   | user  | dbname |     server_id     | router_address | router_time*** | shard_time*** 
--------------+-+-+-+-+-+-
 ************ | user1 | db1    | spqr_shard_1:6432 | local          | ***** | *****
 ************ | user1 | db1    | spqr_shard_2:6432 | local          | ***** | *****
(2 rows)" || {
    echo "where with OR should work"
    exit 1
}

out=$(psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=7432" -c 'show clients where server_id = spqr_shard_1:6432 and dbname = db2;' | clearID | clearStatistics)
test "$out" = " client_id | user | dbname | server_id | router_address | router_time*** | shard_time*** 
-----------+------+--------+-----------+----------------+-----------------+----------------
(0 rows)" || {
    echo "where with AND should work"
    exit 1
}


sleep 20

out=$(psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=7432" -c 'show clients;' | clearID | clearStatistics)
test "$out" = " client_id | user | dbname | server_id | router_address | router_time*** | shard_time*** 
-----------+------+--------+-----------+----------------+-----------------+----------------
(0 rows)"
