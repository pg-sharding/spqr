FROM golang:latest

COPY ./ /spqr
# deps_download avoids the `go mod vendor` pass
RUN cd /spqr && make deps_download && make build GIT_REVISION=devel SPQR_VERSION=devel GIT_COMMIT_COUNT=0
