FROM golang:latest as base


RUN mkdir /router
COPY ./ /router

RUN cd /router && make init && make compile

ENTRYPOINT /router/spqr run -c /router/docker/router/cfg.yaml

