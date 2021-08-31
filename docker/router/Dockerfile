FROM golang:latest as base


RUN mkdir /router
COPY ./ /router

RUN cd /router && make init && make build

ENTRYPOINT /router/spqr run -c /router/docker/router/cfg.yaml || sleep 2332233

