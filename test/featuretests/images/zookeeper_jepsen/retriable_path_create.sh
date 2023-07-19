#!/bin/bash

if [ "$1" == "" ]
then
    echo "Usage $(basename "${0}") <path in zk>"
    exit 1
fi

retry_create() {
    echo "addauth digest testuser:testpassword123
create ${1}
setAcl ${1} auth:testuser:testpassword123:crwad" > /tmp/zk_commands

    tries=0
    ret=1
    while [ ${tries} -le 60 ]
    do
        if cat /tmp/zk_commands | /opt/zookeeper/bin/zkCli.sh
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

retry_create "${1}"
