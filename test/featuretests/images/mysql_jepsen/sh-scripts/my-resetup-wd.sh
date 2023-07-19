#!/bin/bash

set -xe

while true
do
    echo "=============="
    date
    flock -n /tmp/resetup.lock /usr/bin/my-resetup || true
    sleep 10
done
