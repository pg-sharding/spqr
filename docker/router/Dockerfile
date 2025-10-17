FROM spqr-base-image

RUN apk add krb5 postgresql-client
COPY ./docker/router/ssl/localhost.crt /etc/spqr/ssl/server.crt
COPY ./docker/router/ssl/localhost.key /etc/spqr/ssl/server.key
ENTRYPOINT ["/spqr/docker/router/entrypoint.sh"]
