FROM golang:latest as base


RUN mkdir /router
COPY ./ /router

RUN cd /router && make init && make build

ENTRYPOINT /router/spqr run -d /router/docker/router || sleep 2332233

