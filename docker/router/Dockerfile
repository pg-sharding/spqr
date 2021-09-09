FROM golang:1.16


RUN mkdir /router
COPY ./ /router
COPY ./docker/router/qlog /etc/spqr/qlog

RUN cd /router && make init && make build

ENTRYPOINT /router/spqr run -c /router/docker/router/cfg.yaml || sleep 2332233

