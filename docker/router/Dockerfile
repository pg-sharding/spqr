FROM spqr-base-image

ENTRYPOINT CONFIG_PATH=${ROUTER_CONFIG=/spqr/docker/router/cfg.yaml} && CUR_HOST=$(cat ${CONFIG_PATH} | grep "host:") && sed -i "s/${CUR_HOST}/${ROUTER_HOST=${CUR_HOST}}/g" ${CONFIG_PATH} && /spqr/spqr-router run -c ${CONFIG_PATH} --proto-debug
