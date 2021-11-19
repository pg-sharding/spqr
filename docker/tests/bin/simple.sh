#!/bin/bash

sleep 20

set -ex

psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=6432" -c 'CREATE TABLE x(w_id INT)' || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=6432" -c 'insert into x (w_id) values(1)' || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=6432" -c 'insert into x (w_id) values(11)' || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=6432" -c 'select * from x where w_id = 11' || {
	echo "ERROR: tests failed"
	exit 1
}

psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=6432" -c 'select * from x where w_id = 1' || {
	echo "ERROR: tests failed"
	exit 1
}



## tx


psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=6432" << EOH
BEGIN;
INSERT INTO x (w_id) VALUES(5);
ROLLBACK;
EOH

#psql "host=spqr_router_1_1 sslmode=disable user=user1 dbname=db1 port=6432" -c 'select count(1) from x where w_id = 5' || {
#	echo "ERROR: tests failed"
#	exit 1
#}
