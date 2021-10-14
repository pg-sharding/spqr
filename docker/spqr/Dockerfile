FROM golang:1.17

RUN mkdir /router
COPY ./ /router
COPY ./docker/router/qlog /etc/spqr/qlog

RUN cd /router && make && make build


