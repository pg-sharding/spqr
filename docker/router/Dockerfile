FROM spqr-base-image

RUN apt-get update && apt-get install -y --no-install-recommends krb5-user postgresql-client
COPY ./docker/router/ssl/localhost.crt /etc/spqr/ssl/server.crt
COPY ./docker/router/ssl/localhost.key /etc/spqr/ssl/server.key
ENTRYPOINT ["/spqr/docker/router/entrypoint.sh"]
