FROM spqr-base-image

RUN apt-get update && apt-get install -y ragel && apt-get install -y protobuf-compiler
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.32.0
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3.0
RUN go install go.uber.org/mock/mockgen@latest
RUN go install golang.org/x/tools/cmd/goyacc@v0.16.1

ENTRYPOINT cd /spqr && make && make gen