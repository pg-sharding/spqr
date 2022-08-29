FROM golang:1.18

RUN mkdir /spqr
COPY ./ /spqr

RUN cd /spqr && make && make build

