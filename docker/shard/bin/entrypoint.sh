#!/bin/bash
set -ex

rm -fr /var/lib/postgresql/13/main/ 
sudo -u postgres /usr/lib/postgresql/13/bin/pg_ctl -D /var/lib/postgresql/13/main/ init
setup
sudo -u postgres /usr/lib/postgresql/13/bin/postgres -D /var/lib/postgresql/13/main

