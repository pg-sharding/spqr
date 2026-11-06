#!/bin/bash
set -eu

psql_() {
    psql -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -U regress -d regress "$@"
}

# Wait for the test data on every standby used below before promotion.
wal_lsn=$(psql_ -p 6433 -Atqc 'SELECT pg_current_wal_lsn()')
for port in 6434 6436 6437; do
    psql_ -p "$port" -qc "WAIT FOR LSN '$wal_lsn' WITH (TIMEOUT '10s')" > /dev/null
done

psql_ -p 6434 -qc 'SELECT pg_promote(true, 10)'
# The five-node config caches TSA checks for at most 500 ms.
sleep 1
