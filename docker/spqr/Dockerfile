FROM golang:latest

COPY ./ /spqr
RUN cd /spqr && make && make build GIT_REVISION=devel SPQR_VERSION=devel GIT_COMMIT_COUNT=0
