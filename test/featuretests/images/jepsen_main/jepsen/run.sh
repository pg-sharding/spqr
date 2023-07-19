#!/bin/bash

set -e
set -x

cd "$(dirname "$0")"
export LEIN_ROOT=1
for i in zookeeper1 zookeeper2 zookeeper3 mysql1 mysql2 mysql3
do
    ssh-keyscan -t rsa mysync_${i}_1.mysync_mysql_net >> /root/.ssh/known_hosts
done
lein test
