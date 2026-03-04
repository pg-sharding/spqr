FROM golang:alpine

RUN apk add make gcc musl-dev ragel protobuf-dev
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
RUN go install go.uber.org/mock/mockgen@latest
RUN go install golang.org/x/tools/cmd/goyacc@latest

WORKDIR /spqr
COPY go.mod go.sum /spqr/
RUN go mod download

COPY ./ /spqr
RUN CGO_ENABLED=1 make build GIT_REVISION=devel SPQR_VERSION=devel GIT_COMMIT_COUNT=0

ENTRYPOINT make gen