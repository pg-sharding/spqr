FROM golang:alpine

RUN apk add make gcc musl-dev

WORKDIR /spqr
COPY go.mod go.sum /spqr/
RUN go mod download

COPY ./ /spqr
RUN CGO_ENABLED=1 make build GIT_REVISION=devel SPQR_VERSION=devel GIT_COMMIT_COUNT=0