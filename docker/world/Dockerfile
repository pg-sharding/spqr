FROM golang:1.16

RUN mkdir /router
COPY ./ /router

RUN cd /router && make init && make build-world

ENTRYPOINT sleep 2332233

