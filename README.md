[![Go](https://github.com/pg-sharding/spqr/actions/workflows/build.yaml/badge.svg)](https://github.com/pg-sharding/spqr/actions/workflows/build.yaml)
[![Go](https://github.com/pg-sharding/spqr/actions/workflows/tests.yaml/badge.svg)](https://github.com/pg-sharding/spqr/actions/workflows/tests.yaml)
![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/pg-sharding/spqr)
![Go Report](https://goreportcard.com/badge/github.com/pg-sharding/spqr)

# Stateless Postgres Query Router

SPQR is a system for horizontal scaling of PostgreSQL via sharding. We appreciate any kind of feedback and contribution to the project.

For more about SPQR, please see [docs/](docs/).

## Main features

- Transaction and session pooling
- Multiple routers
- Sharding
- Falling unrouted queries to the world shard
- Shard rebalancing
- Limited multi-shard queries
- TLS

## Development

You can use `make run` for a quick example using Docker. For local development you need [the latest Go version](https://go.dev/dl/).

How to build:
```
make
make build
```

How to run:
```
spqr-rr run -c path-to-router-config.yaml
```

## Tests

SPQR does not have unit tests yet but has end-to-end tests. These tests requires Docker and can be run using `make run`. Also, there are stress tests but it work in progress. For more information on testing, please see `test` and `stress` section in [Makefile](./Makefile).

## License

The SPQR source code is distributed under the PostgreSQL Global Development Group License.