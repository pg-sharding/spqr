![Github CI/CD](https://img.shields.io/github/workflow/status/pg-sharding/spqr/Go?logo=github)
![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/pg-sharding/spqr)
![Go Report](https://goreportcard.com/badge/github.com/pg-sharding/spqr)
![Github Repository Size](https://img.shields.io/github/repo-size/pg-sharding/spqr)
![Lines of code](https://img.shields.io/tokei/lines/github/pg-sharding/spqr)

# Stateless Postgres Query Router

## Development

How to build

```
go get golang.org/x/tools/cmd/goyacc
go get -u github.com/golang/protobuf/protoc-gen-go
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc

make yaccgen
make compile
```
