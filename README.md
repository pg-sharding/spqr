[![Go](https://github.com/pg-sharding/spqr/actions/workflows/build.yaml/badge.svg)](https://github.com/pg-sharding/spqr/actions/workflows/build.yaml)
[![Go](https://github.com/pg-sharding/spqr/actions/workflows/tests.yaml/badge.svg)](https://github.com/pg-sharding/spqr/actions/workflows/tests.yaml)
![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/pg-sharding/spqr)
![Go Report](https://goreportcard.com/badge/github.com/pg-sharding/spqr)

# Stateless Postgres Query Router

SPQR is a system for horizontal scaling of PostgreSQL via sharding. We appreciate any kind of feedback and contribution to the project.

For more about SPQR, please see [docs/](docs/) and [benchmarks/](benchmarks/).

## Main features

- Transaction and session pooling
- Multiple routers for fault tolerance
- Sharding 
- Liquid data migrations 
- Limited multi-shard queries
- Works over PostgreSQL protocol
- Falling unrouted queries to the world shard
- [Minor overhead](https://gitlab.com/postgres-ai/postgresql-consulting/tests-and-benchmarks/-/issues/30) for query execution
- and, of course, TLS support

## Development

You can use `make run` for a quick example using Docker. For local development, you need [the latest Go version](https://go.dev/dl/).

How to build:
```
make
make build
```

How to run:
```
spqr-router run -c path-to-router-config.yaml
```

## Tests

SPQR has regression tests. These tests require Docker Compose, and can be run using `make regress`. Also, there are stress tests, but it's a work in progress. For more information on testing, please see `test` and `stress` sections in [Makefile](./Makefile).

## License

The SPQR source code is distributed under the PostgreSQL Global Development Group License.
