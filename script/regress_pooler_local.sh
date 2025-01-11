#!/bin/bash

set -ex

for name in `ls -1 test/regress/tests/pooler/sql/`;
do
	echo $name start
	cat test/regress/tests/pooler/sql/$name |  psql "host=localhost port=6432 dbname=db1" --echo-all --quiet  > test/regress/tests/pooler/expected/$(basename $name .sql).out 2>&1;
	RESULT=$?
	if [ ! $RESULT -eq 0 ]; then 
		exit 1
	fi
	echo $name done
done

killall -9 spqr-router
