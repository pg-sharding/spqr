<h1 align="center">
  <a href="https://pg-sharding.tech/"><img src="docs/logo/spqr_logo-main.svg" alt="Stateless Postgres Query Router" width="400px"></a>
</h1>
<p align="center">
  <b>Stateless Postgres Query Router</b>
</p>

PostgreSQL is awesome, but it's hard to manage a single database with some terabytes of data and 10<sup>5</sup>+ queries per second. Current sharding solutions focus on analytical and hybrid workloads (OLAP, HTAP). Moreover, most of those solutions do not provide a smooth path for monolith to sharded transitions, which is why Yandex Cloud's Data Platform team developed SPQR.

SPQR is a production-ready system for horizontal scaling of PostgreSQL via sharding. We appreciate any kind of feedback and contribution to the project.

To get started or see full list of features, please visit our [documentation](https://pg-sharding.tech).

[![Go](https://github.com/pg-sharding/spqr/actions/workflows/build.yaml/badge.svg)](https://github.com/pg-sharding/spqr/actions/workflows/build.yaml)
[![Go](https://github.com/pg-sharding/spqr/actions/workflows/tests.yaml/badge.svg)](https://github.com/pg-sharding/spqr/actions/workflows/tests.yaml)
![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/pg-sharding/spqr)
![Go Report](https://goreportcard.com/badge/github.com/pg-sharding/spqr)
[![Telegram Chat](https://img.shields.io/badge/telegram-SPQR_dev-blue)](https://t.me/+jMGhyjwicpI3ZWQy)

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

## Support

We have a [Telegram chat](https://t.me/+jMGhyjwicpI3ZWQy) to discuss SPQR usage and development. If you're missing a feature or have found a bug, please open a [GitHub Issue](https://github.com/pg-sharding/spqr/issues/new/choose). We appreciate any kind of feedback and contribution to the project.

## License

The SPQR source code is distributed under the PostgreSQL Global Development Group License.
