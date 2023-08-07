#!/bin/bash
set -ex

POSTGRES_VERSION=$1
if [ "x" == "${POSTGRES_VERSION}x" ]
then
    echo "no postgres version specified"
    exit 1
fi

rm -fr /var/lib/postgresql/$POSTGRES_VERSION/main/ 
sudo -u postgres /usr/lib/postgresql/$POSTGRES_VERSION/bin/pg_ctl -D /var/lib/postgresql/$POSTGRES_VERSION/main/ init
setup
sudo -u postgres /usr/lib/postgresql/$POSTGRES_VERSION/bin/postgres -D /var/lib/postgresql/$POSTGRES_VERSION/main

