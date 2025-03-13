FROM spqr-base-image

RUN apt-get update && apt-get install -y postgresql-client

ENTRYPOINT /spqr/spqr-coordinator -c ${COORDINATOR_CONFIG=/spqr/docker/coordinator/cfg.yaml} >> ./coord.log
