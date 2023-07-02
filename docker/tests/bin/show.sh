#!/bin/bash

sleep 20

set -ex

function clearID() {
    sed -E 's/0x[0-9a-f]+/************/g'
}

out=$(psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=7432" -c 'show clients;' | clearID)
test "$out" = " client id | user | dbname | server_id 
-----------+------+--------+-----------
(0 rows)"

psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=6432" <<EOH &
select 1;
SELECT pg_sleep(20);
select 1;
EOH

sleep 10

out=$(psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=7432" -c 'show clients;' | clearID)
test "$out" = "  client id   | user  | dbname |     server_id     
--------------+-------+--------+-------------------
 ************ | user1 | db1    | spqr_shard_1:6432
 ************ | user1 | db1    | spqr_shard_2:6432
(2 rows)"

sleep 20

out=$(psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=7432" -c 'show clients;' | clearID)
test "$out" = " client id | user | dbname | server_id 
-----------+------+--------+-----------
(0 rows)"
