#!/bin/bash
set -eu

psql_() {
    psql -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -U regress -d regress "$@"
}

# Wait for the test data to reach the standby before promoting it.
wal_lsn=$(psql_ -p 6433 -Atqc 'SELECT pg_current_wal_lsn()')
for ((attempt = 0; attempt < 100; attempt++)); do
    if [ "$(psql_ -p 6434 -Atqc "SELECT pg_last_wal_replay_lsn() >= '$wal_lsn'::pg_lsn")" = t ]; then
        psql_ -p 6434 -qc 'SELECT pg_promote(true, 10)'
        # The five-node config caches TSA checks for at most 500 ms.
        sleep 1
        exit 0
    fi
    sleep 0.1
done

echo 'standby did not replay the test data' >&2
exit 1
