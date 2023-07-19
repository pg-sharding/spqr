#!/bin/bash
set -ex

supervisorctl stop zookeeper
ps -aux | grep [z]oo.cfg | awk '{print $2}' | xargs kill || true
/var/lib/dist/base/generate_certs.sh $1
supervisorctl start zookeeper
