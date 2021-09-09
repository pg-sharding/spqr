[![Go](https://github.com/pg-sharding/spqr/actions/workflows/build.yaml/badge.svg)](https://github.com/pg-sharding/spqr/actions/workflows/build.yaml)
[![Go](https://github.com/pg-sharding/spqr/actions/workflows/tests.yaml/badge.svg)](https://github.com/pg-sharding/spqr/actions/workflows/tests.yaml)
![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/pg-sharding/spqr)
![Go Report](https://goreportcard.com/badge/github.com/pg-sharding/spqr)
![Lines of code](https://img.shields.io/tokei/lines/github/pg-sharding/spqr)

# Stateless Postgres Query Router

## Development

How to build

```
go get golang.org/x/tools/cmd/goyacc
go get -u github.com/golang/protobuf/protoc-gen-go
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc

make yaccgen
make build
```

Check it out

```
docker-compose up
```
