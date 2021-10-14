FROM shardbase
FROM golang:1.17 as worldbase

RUN mkdir /router
COPY ./ /router

RUN cd /router && make init && make build-world

ENTRYPOINT sleep 2332233

