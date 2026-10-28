#!/bin/bash

ro=false

while [[ "$#" -gt 0 ]]; do
    case $1 in
        -r|--router-only) ro="$2"; shift ;;
        *) echo "Unknown parameter: $1"; exit 1 ;;
    esac
    shift
done

if ! $ro; then

	for name in `ls -1 test/regress/tests/common/sql/`;
	do
		echo $name start
		cat test/regress/tests/common/sql/$name |  psql "host=localhost port=6432 dbname=spqr-console" --echo-all --quiet > test/regress/tests/common/expected/$(basename $name .sql).out 2>&1;
		RESULT=$?
		if [ ! $RESULT -eq 0 ]; then 
			exit 1
		fi
		echo $name done
	done

	for name in `ls -1 test/regress/tests/coordinator/sql/`;
	do
		echo $name start
		cat test/regress/tests/coordinator/sql/$name |  psql "host=localhost port=6432 dbname=spqr-console" --echo-all --quiet > test/regress/tests/coordinator/expected/$(basename $name .sql).out 2>&1;
		RESULT=$?
		if [ ! $RESULT -eq 0 ]; then 
			exit 1
		fi
		echo $name done
	done

	for name in `ls -1 test/regress/tests/console/sql/`;
	do
		echo $name start
		cat test/regress/tests/console/sql/$name |  psql "host=localhost port=6432 dbname=spqr-console" --echo-all --quiet > test/regress/tests/console/expected/$(basename $name .sql).out 2>&1;
		RESULT=$?
		if [ ! $RESULT -eq 0 ]; then 
			exit 1
		fi
		echo $name done
	done
fi

for name in `ls -1 test/regress/tests/router/sql/`;
do
	echo $name start
	cat test/regress/tests/router/sql/$name |  psql "host=localhost port=6432 dbname=db1" --echo-all --quiet > test/regress/tests/router/expected/$(basename $name .sql).out 2>&1;
	RESULT=$?
	if [ ! $RESULT -eq 0 ]; then 
		exit 1
	fi
	echo $name done
done

killall spqr-router
