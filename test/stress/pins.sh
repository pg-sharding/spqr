#!/bin/bash

export CLIENTS=10

function tester {
	n=$(for i in `seq 1 100`; do
			if [[ i -eq 1 ]]; then
				echo 'SET __spqr__session_connections_pin TO on;';
			else
				echo 'select pg_backend_pid() /*__spqr__execute_on: sh1 */;';
			fi; done 2>&1 | psql "host=stress_router port=6432 dbname=db1 user=user1" -t | sort | uniq -d | wc  -l)
	echo $n

	if [[ n -ne 2 ]]; then 
		exit 1
	fi
}



pids=()

for i in `seq 1 $CLIENTS`;
do 
	tester & pids+=($!)
done

# Await each specific task to capture errors
for pid in "${pids[@]}"; do
    if ! wait "$pid"; then
        echo "Process $pid failed!"
    fi
done

echo 'done'
