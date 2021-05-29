#!/bin/bash

/home/reshke/bins/pgsql/bin/initdb /tmp/DemoDb3 && /home/reshke/bins/pgsql/bin/pg_ctl -o "-p 5432" -D /tmp/DemoDb3 -l /tmp/pg_log.log start
/home/reshke/bins/pgsql/bin/initdb /tmp/DemoDb4 && /home/reshke/bins/pgsql/bin/pg_ctl -o "-p 5442" -D /tmp/DemoDb4 -l /tmp/pg_log.log start

psql "host=localhost port=5432 sslmode=disable" -f /home/reshke/code/shgo/sql/init.sql
psql "host=localhost port=5442 sslmode=disable" -f /home/reshke/code/shgo/sql/init.sql

