#!/bin/bash

for name in `ls -1 test/regress/tests/router/sql/`;
do
	cat test/regress/tests/router/sql/$name |  psql "host=localhost port=6432 dbname=db1" --echo-all --quiet > test/regress/tests/router/expected/$(basename $name .sql).out 2>&1;
done

for name in `ls -1 test/regress/tests/console/sql/`;
do
	cat test/regress/tests/console/sql/$name |  psql "host=localhost port=6432 dbname=spqr-console" --echo-all --quiet > test/regress/tests/console/expected/$(basename $name .sql).out 2>&1;
done


killall -9 spqr-router
