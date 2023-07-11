# SPQR benchmarks

## Test #1

The main goal of the test was to show that adding one more shard is not increasing the query latency. On the one hand, it is a very obvious statement but on the other hand, we need to make sure of this.

### Setup

In this test I benchmarked an SPQR installation with only one `spqr-router`:
- I use `sysbench` and it's out of box OLTP test
- `sysbench` and `spqr-router` are running on the same host
- Each shard is PostgreSQL 14 with 8 vCPU, 100% vCPU rate, 32 GB RAM, 100 GB local SSD disk
- Host with the router has 16 vCPU, 100% vCPU rate, 32 GB RAM, 100 GB local SSD disk
- I ran this test with [2,4,8,16,32,64,128] shards

I used `config.py` script to generate the router config and `init.py` to generate the `init.sql` (SQL-like code that creates key ranges).

The router config was like this:

```
log_level: error
host: localhost
router_port: '6432'
admin_console_port: '7432'
grpc_api_port: '7000'
world_shard_fallback: false
show_notice_messages: false
init_sql: init.sql
router_mode: PROXY
frontend_rules:
  - db: denchick
    usr: denchick
    auth_rule:
      auth_method: ok
      password: ''
    pool_mode: SESSION
    pool_discard: false
    pool_rollback: false
    pool_prepared_statement: true
    pool_default: false
frontend_tls:
  sslmode: disable
backend_rules:
  - db: denchick
    usr: denchick
    auth_rule:
      auth_method: md5
      password: password
    pool_default: false
shards:
  shard01:
  ...

```

For creating shards I used [Managed Service for PostgreSQL](https://cloud.yandex.com/en/services/managed-postgresql) and its [terraform provider](https://registry.terraform.io/providers/yandex-cloud/yandex/), see `main.tf`.

### Results

The query latency is indeed not increased. Test outputs are stored in `results/` folder.

## Test #2

The main goal of the test was to compare the query latency with and without using `spqr-router`.

### Setup

- I created a Managed PostgreSQL 14 Cluster with 8 vCPU, 100% vCPU rate, and 16 GB RAM
- I use `sysbench` and it's out of box OLTP tests (see `results/` folder)
- Test data is 100 tables with 10 000 000 rows in each table
- `sysbench` and `spqr-router` are running on the same host
- Host with the router has the same resources as Postgres

I made two runs:

1. connecting directly to the cluster
2. connecting via the router.

### Results

Raw Postgres could process **402.42 transactions per second**, and the router made **373.76 tps**. The difference is about 10%. Test outputs are stored in `results/` folder.

## Test #3

TODO TPC-C test

