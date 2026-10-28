ARG POSTGRES_VERSION=17

FROM spqr-shard-image

ARG POSTGRES_VERSION
ENV POSTGRES_VERSION=$POSTGRES_VERSION
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends iptables

# router binary comes from the prebuilt base image
COPY --from=spqr-base-image /spqr/spqr-router /spqr/spqr-router

COPY ./docker/5node/entrypoint.sh /usr/local/bin/entrypoint-5node.sh
COPY ./test/regress/conf/router-5node.yaml /etc/spqr/router-5node.yaml

RUN chmod +x /usr/local/bin/entrypoint-5node.sh

ENTRYPOINT ["/usr/local/bin/entrypoint-5node.sh"]