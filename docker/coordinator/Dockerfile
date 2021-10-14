FROM golang:1.17

RUN mkdir /router
COPY ./ /router

RUN cd /router && make init && make build_c

ENTRYPOINT sleep 2332233

