#!/bin/bash

set -e

function handle_term()
{
    ps aux | grep '[s]pqr-router run' | awk '{print $2}' | xargs kill -TERM
    wait -n || exit $?
    exit
}

trap 'handle_term' TERM
trap
CONFIG_PATH=${ROUTER_CONFIG=/spqr/docker/router/cfg.yaml}
COORD_CONFIG_PATH=${COORDINATOR_CONFIG=/spqr/docker/coordinator/cfg.yaml}
CUR_HOST=$(cat ${CONFIG_PATH} | grep "host:")
sed "s/${CUR_HOST}/${ROUTER_HOST=${CUR_HOST}}/g" -i ${CONFIG_PATH}
rm -f /tmp/.s.PGSQL.*
echo ${CONFIG_PATH}
/go/bin/dlv --listen=:2345 --headless=true --log=true --accept-multiclient --log-output=rpc,dap --check-go-version=true exec /spqr/spqr-router -- run --config ${CONFIG_PATH} --coordinator-config ${COORD_CONFIG_PATH} &
while true; do sleep 1; done

