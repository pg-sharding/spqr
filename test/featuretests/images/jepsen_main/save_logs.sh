#!/bin/bash

for i in 1 2 3
do
    mkdir -p tests/logs/mysql${i}
    mkdir -p tests/logs/zookeeper${i}

    for logfile in /var/log/mysync.log /var/log/mysql/error.log /var/log/mysql/query.log /var/log/resetup.log /var/log/supervisor.log
    do
        logname=$(echo "${logfile}" | rev | cut -d/ -f1 | rev)
        docker exec mysync_mysql${i}_1 cat "${logfile}" > "tests/logs/mysql${i}/${logname}"
    done

    docker exec mysync_zoo${i}_1 cat /var/log/zookeeper/zookeeper--server-mysync_zookeeper${i}_1.log > tests/logs/zookeeper${i}/zk.log 2>&1
done

tail -n 18 tests/logs/jepsen.log
# Explicitly fail here
exit 1
