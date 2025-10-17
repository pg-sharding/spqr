FROM golang:1.25-alpine AS gobuild
RUN apk add --no-cache gcc musl-dev linux-headers make
ENV CGO_ENABLED=1
WORKDIR /spqr
COPY go.mod .
COPY go.sum .
RUN go mod download
COPY . .
RUN cd /spqr && make build GIT_REVISION=devel SPQR_VERSION=devel
