FROM spqr-base-image

RUN apt-get update && apt-get install -y postgresql-client

ENTRYPOINT CONFIG_PATH=${ROUTER_CONFIG=/spqr/docker/router/cfg.yaml} COORD_CONFIG_PATH=${COORDINATOR_CONFIG=/spqr/docker/coordinator/cfg.yaml} && CUR_HOST=$(cat ${CONFIG_PATH} | grep "host:") && sed -i "s/${CUR_HOST}/${ROUTER_HOST=${CUR_HOST}}/g" ${CONFIG_PATH} && /spqr/spqr-router run --config ${CONFIG_PATH} --coordinator-config ${COORD_CONFIG_PATH}
