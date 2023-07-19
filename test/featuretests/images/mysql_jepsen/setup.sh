#!/bin/bash

set -ex

retry_mysql_query() {
    tries=0
    ret=1
    while [ ${tries} -le 60 ]
    do
        if (echo "${1}" | mysql test1)
        then
            ret=0
            break
        else
            tries=$(( tries + 1 ))
            sleep 1
        fi
    done
    return ${ret}
}

if ! retry_mysql_query "CREATE TABLE IF NOT EXISTS test1.test_set(value int) ENGINE=INNODB;"
then
    exit 1
fi
