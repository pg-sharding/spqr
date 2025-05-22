#!/bin/bash
set -ex

POSTGRES_VERSION=$1
if [ "x" == "${POSTGRES_VERSION}x" ]
then
    echo "no postgres version specified"
    exit 1
fi

rm -fr /var/lib/postgresql/$POSTGRES_VERSION/main/ 

if [ "x" != "x$PG_MASTER" ]; then
    until sudo -u postgres psql -h $PG_MASTER -d postgres -p 6432 -c "SELECT pg_drop_replication_slot('replication_slot');"; sudo -u postgres psql -h $PG_MASTER -d postgres -p 6432 -c "SELECT pg_create_physical_replication_slot('replication_slot');"
    do
        echo 'Waiting for replication slot creation'
        sleep 1s
    done
    until sudo -u postgres pg_basebackup --pgdata=/var/lib/postgresql/$POSTGRES_VERSION/main -R --slot=replication_slot --host=$PG_MASTER --port=6432
    do
        echo 'Waiting for primary to connect...'
        sleep 1s
    done
    sudo -u postgres chmod 0700 /var/lib/postgresql/$POSTGRES_VERSION/main
    sudo -u postgres /usr/lib/postgresql/$POSTGRES_VERSION/bin/postgres -D /var/lib/postgresql/$POSTGRES_VERSION/main
else
    sudo -u postgres /usr/lib/postgresql/$POSTGRES_VERSION/bin/pg_ctl -D /var/lib/postgresql/$POSTGRES_VERSION/main/ init
    setup
    sudo -u postgres /usr/lib/postgresql/$POSTGRES_VERSION/bin/postgres -D /var/lib/postgresql/$POSTGRES_VERSION/main
fi
