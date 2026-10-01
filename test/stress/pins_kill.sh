#!/bin/bash

# Connection pin under mid-transaction disconnects.

set -u

export CLIENTS=6

CONN_STR="host=stress_router port=6432 dbname=stress user=stress"

# wait until some victim is blocked in pg_sleep on a shard backend
wait_in_tx() {
	for _ in $(seq 1 15); do
		n=$(psql "$CONN_STR" -qAt -c "select count(*) /*__spqr__execute_on: sh1*/ from pg_stat_activity where state = 'active' and wait_event in ('PgSleep', 'Delay');" 2>/dev/null)
		if [[ "$n" -ge 1 ]]; then
			return 0
		fi
		sleep 1
	done
	return 1
}

kill_mid_tx() {
	psql "$CONN_STR" -qAt >/dev/null 2>&1 <<'EOF' &
SET __spqr__session_connections_pin TO on;
SELECT pg_backend_pid() /*__spqr__execute_on: sh1*/;
BEGIN;
SELECT pg_backend_pid() /*__spqr__execute_on: sh1*/;
SELECT pg_sleep(10) /*__spqr__execute_on: sh1*/;
EOF
	local victim=$!

	sleep 1
	wait_in_tx || echo "warning: victim ${victim} was not observed inside a transaction" >&2

	kill -9 "${victim}" 2>/dev/null
	wait "${victim}" 2>/dev/null
}

check_fresh_session() {
	local out

	out=$(psql "$CONN_STR" -qAt 2>/dev/null <<'EOF'
SET __spqr__session_connections_pin TO on;
SELECT pg_backend_pid() /*__spqr__execute_on: sh1*/;
BEGIN;
SELECT pg_backend_pid() /*__spqr__execute_on: sh1*/;
SELECT 1 /*__spqr__execute_on: sh1*/;
COMMIT;
SELECT pg_backend_pid() /*__spqr__execute_on: sh1*/;
EOF
)

	# three identical pids and a "1" row
	if [[ $(printf '%s\n' "$out" | wc -l) -ne 4 ]] ||
		! printf '%s\n' "$out" | grep -qx 1 ||
		[[ $(printf '%s\n' "$out" | sort -u | wc -l) -ne 2 ]] ||
		[[ $(printf '%s\n' "$out" | grep -cx "$(printf '%s\n' "$out" | sed -n 1p)") -ne 3 ]]; then
		echo "unexpected output: $out" >&2
		exit 1
	fi
}

function tester {
	for _ in 1 2 3; do
		kill_mid_tx
		sleep 1
		check_fresh_session
	done
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
        exit 1
    fi
done

# the router must stay alive while the killed clients' relays unwind
# and leave no backend inside a transaction on sh1
for i in $(seq 1 25); do
	n=$(psql "$CONN_STR" -qAt -c "select count(*) /*__spqr__execute_on: sh1*/ from pg_stat_activity where state = 'idle in transaction';" 2>/dev/null)
	if ! [[ "$n" =~ ^[0-9]+$ ]]; then
		echo "router is not serving: $n" >&2
		exit 1
	fi
	sleep 1
done

if [[ "$n" != 0 ]]; then
	echo "backends leaked in transaction: $n" >&2
	exit 1
fi

echo 'done'
