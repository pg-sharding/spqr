[![Go](https://github.com/pg-sharding/spqr/actions/workflows/build.yaml/badge.svg)](https://github.com/pg-sharding/spqr/actions/workflows/build.yaml)
[![Go](https://github.com/pg-sharding/spqr/actions/workflows/tests.yaml/badge.svg)](https://github.com/pg-sharding/spqr/actions/workflows/tests.yaml)
![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/pg-sharding/spqr)
![Go Report](https://goreportcard.com/badge/github.com/pg-sharding/spqr)
[![Telegram Chat](https://img.shields.io/badge/telegram-SPQR_dev-blue)](https://t.me/+jMGhyjwicpI3ZWQy)

# Stateless Postgres Query Router

PostgreSQL is awesome, but it's hard to manage a single database with some terabytes of data and 10<sup>5</sup>+ queries per second. Current sharding solutions focus on analytical and hybrid workloads (OLAP, HTAP). Moreover, most of those solutions do not provide a smooth path for monolith to sharded transitions, which is why Yandex Cloud's Data Platform team developed SPQR.

SPQR is a production-ready system for horizontal scaling of PostgreSQL via sharding. We appreciate any kind of feedback and contribution to the project.

For more about SPQR, please see [docs/](docs/).

## Main features

SPQR works well when most of your queries can be executed on one shard.

- **Sharding**. If possible, the router tries to determine on the first transaction statement to which shard this transaction should be sent. But you can explicitly specify a shard or a [sharding key](https://github.com/pg-sharding/spqr/blob/master/test/regress/tests/router/expected/routing_hint.out#L30) in a comment request.
- **Transaction and session pooling**. Just as in your favorite connection pooler (Odyssey or PgBouncer).
- **Multiple routers for fault tolerance**. The router stores the sharding rules only for cache purposes. Information about the entire installation is stored inside the QDB service, so the number of routers running simultaneously is unlimited.
- **Liquid data migrations**. Data migration between shards aims to balance the workload across shards proportionally. The main idea is to minimize any locking impact during these migrations, which is accomplished by reducing the size of the data ranges being transferred.
- **Limited cross-shard queries**. SPQR router supports limited subset of cross-shard queries. This is made from best-effort logic in a non-disruptive and non-consistent way and is used mainly for testing purposes. Please do not use this feature in your production, cross-shard snapshot will be inconsistent.
- **Multiple servers and failover**. In the router configuration, it is possible to specify multiple servers for one shard. Then the router will distribute read-only queries among the replicas. However, in addition to the automatic routing, you also have the option to explicitly define the destination for a specific query by using the [target-session-attr](https://github.com/pg-sharding/spqr/blob/master/test/regress/tests/router/expected/target_session_attrs.out#L32) parameter within the query.
- **Works over PostgreSQL protocol**. It means you can connect to the router and the coordinator via psql to perform administrative tasks.
- **Dedicated read-only mode**. Once enabled, the router will respond to a SHOW transaction_read_only command with "true" and handle only read-only queries, similar to a standard PostgreSQL replica.
- **Minor overhead for query execution**. See benchmarks [here](docs/Benchmarks.md) and [here](https://gitlab.com/postgres-ai/postgresql-consulting/tests-and-benchmarks/-/issues/30).
- **Varias authentication types**. From basic OK and plain text to MD5 and SCRAM, see [Authentication.md](docs/Authentication.md).
- **Live configuration reloading**. You can send a SIGHUP signal to the router's process. This will trigger the router to reload its configuration file and apply any changes without interrupting its operation.
- **Statistics**. You can get access to statitics in router's administrative console via [SHOW command](https://github.com/pg-sharding/spqr/blob/master/yacc/console/gram.y#L319). 
- **Fallback execution of unrouted queries to the "world shard"**. SPQR is optimized for single-shard OLTP queries. But we have long-term plans to support routing queries for 2 or more shards.

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

SPQR has all types of tests: unit, regress, and end-to-end. These tests require Docker Compose, and can be run using `make`. For more information on testing, please see `unittest`, `regress`, and `feature` sections in [Makefile](./Makefile).

## License

The SPQR source code is distributed under the PostgreSQL Global Development Group License.

## Chat

We have a Telegram chat to discuss SPQR usage and development, to join use [invite link](https://t.me/+jMGhyjwicpI3ZWQy).
