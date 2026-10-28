#!/bin/bash

export CLIENTS=20

function tester {
	# For each client, repeatedly acquire and release an advisory lock.
	# pg_advisory_unlock must always return true, meaning the lock was
	# held by this session and successfully released. If any unlock
	# returns false ("f"), the router didn't pin connection
	n=$(for i in `seq 1 10`; do
			if [[ i -eq 1 ]]; then
				echo 'SET __spqr__linearize_dispatch TO true;';
				echo 'SET __spqr__engine_v2 TO true;';
				echo 'SET __spqr__advisory_lock_behaviour TO SCATTER;';
			else
				echo "select pg_advisory_lock($i);"
				echo "show __spqr__session_connections_pin;";
				echo "select pg_advisory_unlock($i);"
			fi;
		done 2>&1 | psql "host=stress_router port=6432 dbname=stress user=stress" -t -A | grep -c '^f$')
	echo $n

	if [[ n -ne 0 ]]; then
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
