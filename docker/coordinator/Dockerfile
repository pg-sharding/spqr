FROM spqr-base-image

RUN apt-get update && apt-get install -y postgresql-client
COPY ./docker/router/ssl/localhost.crt /etc/spqr/ssl/server.crt
COPY ./docker/router/ssl/localhost.key /etc/spqr/ssl/server.key
ENTRYPOINT /spqr/spqr-coordinator -c ${COORDINATOR_CONFIG=/spqr/docker/coordinator/cfg.yaml} >> ./coord.log
