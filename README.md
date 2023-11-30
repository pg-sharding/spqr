[![Go](https://github.com/pg-sharding/spqr/actions/workflows/build.yaml/badge.svg)](https://github.com/pg-sharding/spqr/actions/workflows/build.yaml)
[![Go](https://github.com/pg-sharding/spqr/actions/workflows/tests.yaml/badge.svg)](https://github.com/pg-sharding/spqr/actions/workflows/tests.yaml)
![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/pg-sharding/spqr)
![Go Report](https://goreportcard.com/badge/github.com/pg-sharding/spqr)
[![Telegram Chat](https://img.shields.io/badge/telegram-SPQR_dev-blue)](https://t.me/+jMGhyjwicpI3ZWQy)

# Stateless Postgres Query Router

PostgreSQL is awesome, but it's hard to manage a single database with some terabytes of data and 105+ queries per second. Existing sharding solutions focus on analytical and hybrid workloads (OLAP, HTAP). Moreover, most of those solutions do not provide a simple, painless path for the monolith<->sharded transitions. That's why the Data Platform team of Yandex.Cloud designed SPQR.

SPQR is a production-ready system for horizontal scaling of PostgreSQL via sharding. We appreciate any kind of feedback and contribution to the project.

For more about SPQR, please see [docs/](docs/).

## Main features

SPQR works well when you do not have queries that can be loaded strictly on one shard.

- **Sharding**. If it possible, the router tries to determine on the first transaction statement to which shard this transaction should be sent. But you can explicitly specify a shard in a comment request.
- **Transaction and session pooling**. Just right in your favorite connection poller (Odyssey or PgBouncer).
- **Multiple routers for fault tolerance**. The router stores the sharding rules only for cache purposes, information about the entire installation is stores inside the QDB service, so the number of routers running simultaneously is unlimited.
- **Liquid data migrations**. Data migration between shards aims to balance the workload across shards proportionally. The main idea is to minimize any locking impact during these migrations, which is accomplished by reducing the size of the data ranges being transferred.
- **Limited cross-shard queries**. SPQR router support limited cross-shard queries. This is made from best-effort logic in a non-disruptive and non-consistent way and is used mainly for testing purposes. Please do not use this in your production.
- **Works over PostgreSQL protocol**. Router and coordinator work on the PostgreSQL protocol. This means that you can connect to them via psql.
- **Minor overhead for query execution**. We have some benchmarks [here](docs/Benchmarks.md) and [here](https://gitlab.com/postgres-ai/postgresql-consulting/tests-and-benchmarks/-/issues/30).
- **Varias authentication types**. From basic OK and plain text to MD5 and SCRUM, see [Authentication.md](docs/Authentication.md).
- *Falling unrouted queries to the world shard*. SPQR is optimized for single-shard OLTP queries. But we have long-term plans to support routing queries for 2 or more shards.

## Development

You can use `make run` for a quick example using Docker. For local development, you need [the latest Go version](https://go.dev/dl/).

How to build:
```
make
make build
```

How to run:
```
spqr-router run --config path-to-router-config.yaml
```

## Tests

SPQR has all types of tests: unit, regress and, end-to-end. These tests require Docker Compose, and can be run using `make`. For more information on testing, please see `unittest`, `regress`, and `feature` sections in [Makefile](./Makefile).

## License

The SPQR source code is distributed under the PostgreSQL Global Development Group License.

## Chat

We have a Telegram chat to discuss SPQR usage and development, to join use [invite link](https://t.me/+jMGhyjwicpI3ZWQy).
