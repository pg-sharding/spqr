FROM golang:1.17

RUN mkdir /router
COPY ./ /router

RUN cd /router && make && make build

