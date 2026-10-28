FROM golang:1.26.0

RUN apt-get update \
    && apt-get install -y --no-install-recommends libprotobuf-dev protobuf-compiler ragel \
    && rm -rf /var/lib/apt/lists/*
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest \
    && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest \
    && go install go.uber.org/mock/mockgen@latest \
    && go install golang.org/x/tools/cmd/goyacc@latest

WORKDIR /spqr

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ENTRYPOINT ["make", "gen"]
