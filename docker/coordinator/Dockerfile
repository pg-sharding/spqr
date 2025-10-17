FROM spqr-base-image

RUN apk add postgresql-client

ENTRYPOINT /spqr/spqr-coordinator -c ${COORDINATOR_CONFIG=/spqr/docker/coordinator/cfg.yaml} >> ./coord.log
