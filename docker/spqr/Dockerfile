FROM golang:1.18

RUN mkdir /router
COPY ./ /router

RUN cd /router && make && make build

