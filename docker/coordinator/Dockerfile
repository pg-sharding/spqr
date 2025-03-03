FROM spqr-base-image

RUN apt-get update && apt-get install -y postgresql-client

ENTRYPOINT CONFIG_PATH=${COORDINATOR_CONFIG=/spqr/docker/coordinator/cfg.yaml} /spqr/spqr-coordinator --config ${CONFIG_PATH}