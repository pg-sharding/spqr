FROM golang:1.20

COPY ./ /spqr
RUN cd /spqr && make && make build GIT_REVISION=devel SPQR_VERSION=devel