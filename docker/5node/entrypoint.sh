#!/bin/bash
set -ex

if [ "x$POSTGRES_VERSION" == "x" ]; then
    echo "no postgres version specified"
    exit 1
fi

export POSTGRES_USER=${POSTGRES_USER:-regress}
export POSTGRES_DB=${POSTGRES_DB:-regress}
export POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-12345678}

BIN=/usr/lib/postgresql/$POSTGRES_VERSION/bin
DATA=/var/lib/postgresql/$POSTGRES_VERSION
PRIMARY_PORT=${PRIMARY_PORT:-6433}
REPLICA_PORTS="${REPLICA_PORTS:-6434 6435 6436 6437}"
ROUTER_CONFIG=${ROUTER_CONFIG:-/etc/spqr/router-5node.yaml}

psql_() { sudo -u postgres "$BIN/psql" -h 127.0.0.1 -p "$PRIMARY_PORT" -U postgres "$@"; }

rm -rf "$DATA"
mkdir -p "$DATA"
chown postgres:postgres "$DATA"

# primary on $PRIMARY_PORT
sudo -u postgres "$BIN/initdb" -D "$DATA/main" --no-locale --encoding=UTF8 -A trust

cat >> "$DATA/main/postgresql.conf" <<EOF
port = $PRIMARY_PORT
listen_addresses = '127.0.0.1'
unix_socket_directories = '/tmp'
logging_collector = off
max_prepared_transactions = 5
shared_preload_libraries = 'pg_stat_statements,pg_stat_kcache,pg_comment_stats,spqrguard,twopc_aux_tester'
wal_keep_size = 512MB
max_wal_senders = 10
EOF

sudo -u postgres "$BIN/pg_ctl" -D "$DATA/main" -o "-p $PRIMARY_PORT" start

psql_ -c "CREATE ROLE $POSTGRES_USER WITH LOGIN SUPERUSER PASSWORD '$POSTGRES_PASSWORD';"
psql_ -c "CREATE DATABASE $POSTGRES_DB OWNER $POSTGRES_USER;"

for ext in postgres_fdw pg_stat_statements pg_stat_kcache pg_comment_stats; do
    psql_ -d "$POSTGRES_DB" -c "CREATE EXTENSION IF NOT EXISTS $ext;"
done
psql_ -d "$POSTGRES_DB" -c "CREATE EXTENSION IF NOT EXISTS spqrhash VERSION '1.2';"
psql_ -d "$POSTGRES_DB" -c "CREATE EXTENSION IF NOT EXISTS spqrguard VERSION '2.3';"

# replicas
i=0
for port in $REPLICA_PORTS; do
    slot="replication_slot_$((i + 1))"
    psql_ -c "SELECT pg_create_physical_replication_slot('$slot');"
    sudo -u postgres "$BIN/pg_basebackup" -h 127.0.0.1 -p "$PRIMARY_PORT" -D "$DATA/replica_$i" -R --slot="$slot" -U postgres
    cat >> "$DATA/replica_$i/postgresql.auto.conf" <<EOF
port = $port
listen_addresses = '127.0.0.1'
EOF
    chown -R postgres:postgres "$DATA/replica_$i"
    sudo -u postgres "$BIN/pg_ctl" -D "$DATA/replica_$i" -o "-p $port" start
    i=$((i + 1))
done

/spqr/spqr-router run --config "$ROUTER_CONFIG" >> "${ROUTER_LOG:-/var/log/spqr-router.log}" 2>&1 &

while true; do sleep 3600; done