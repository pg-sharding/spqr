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

out=$(psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=7432" -c 'show clients;' | clearID | clearStatistics)
test "$out" = " client_id | user | dbname | server_id | router_time*** | shard_time*** 
-----------+------+--------+-----------+-----------------+----------------
(0 rows)"

psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=6432" <<EOH &
select 1;
SELECT pg_sleep(20);
EOH

sleep 10

clientID=$(psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=7432" -c 'show clients;' --csv | head -2 | tail -1 | awk -F ',' '{print $1 }')

out=$(psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=7432" -c "kill client $clientID;" | clearID)
test "$out" = "            kill client             
------------------------------------
 the client ************ was killed
(1 row)"

out=$(psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=7432" -c 'show clients;' | clearID | clearStatistics)
test "$out" = " client_id | user | dbname | server_id | router_time*** | shard_time*** 
-----------+------+--------+-----------+-----------------+----------------
(0 rows)"

out=$(psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=7432" -c "kill client $clientID;" 2>&1 | clearID)
test "$out" = "
		SQPR router admin console
	Here you can configure your routing rules
------------------------------------------------
	You can find documentation here 
https://github.com/pg-sharding/spqr/tree/master/docs

ERROR:  No such client ************"
