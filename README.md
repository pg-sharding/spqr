[![Go](https://github.com/pg-sharding/spqr/actions/workflows/build.yaml/badge.svg)](https://github.com/pg-sharding/spqr/actions/workflows/build.yaml)
[![Go](https://github.com/pg-sharding/spqr/actions/workflows/tests.yaml/badge.svg)](https://github.com/pg-sharding/spqr/actions/workflows/tests.yaml)
![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/pg-sharding/spqr)
![Go Report](https://goreportcard.com/badge/github.com/pg-sharding/spqr)
![Lines of code](https://img.shields.io/tokei/lines/github/pg-sharding/spqr)

# Stateless Postgres Query Router

## Overview
SPQR is an OLTP sharding solution for PostgreSQL.

PostgreSQL is awesome, but it's hard to manage single database with some terabytes of data and 10<sup>5</sup>+ queries per second. Existing sharding solutions focus on analytical workloads. Also most of solutions neglect pains of monolith<->sharded transitions. That's why [Data Platform](https://cloud.yandex.com/en-ru/services#data-platform) team of Yandex.Cloud designed SPQR.

###Core requirements that formed SPQR design
1. Using highly available clusters as building blocks for sharding installations. These clusters can be based on Patroni, Stolon, Managed PostgreSQL or any other HA solution over vanilla Postgres. Physical quorum-based PostgreSQL HA solutions are battle proven and we are reusing all their benefits.
2. Zero downtime for conversion from monolith to sharded cluster and vice versa. Your existing database if first shard o horizontal scaling. And if at some point you want to go back - you do not need to restore from a backup.
3. SPQR dev environment should be installable on developers laptop or Raspberry Pi in minutes, not hours in a datacenter.
4. SPQR is optimised for single-shard queries.

## Development

How to build

```
make
make build
```

Try it

```
make run

.......


Configute routing rules


docker-compose run --entrypoint /bin/bash client
root@spqr_client:/go# connect_adm.sh


		SQPR router admin console

	Here you can configure your routing rules
------------------------------------------------

	You can find documentation here
https://github.com/pg-sharding/spqr/tree/master/doc/router


psql (13.4 (Debian 13.4-4.pgdg110+1), server console)
Type "help" for help.

db1=?> CREATE SHARDING COLUMN w_id;
                   fortune
----------------------------------------------
 created sharding column w_id, err %!w(<nil>)
(1 row)

db1=?> ADD KEY RANGE 1 10 sh1 krid1;
                      fortune
---------------------------------------------------
 created key range from [49] to [49 48], err <nil>
(1 row)

db1=?> ADD KEY RANGE 11 20 sh2 krid2;
                       fortune
------------------------------------------------------
 created key range from [49 49] to [50 48], err <nil>
(1 row)

db1=?>

```

Your sharding routes are configured. Now you can execute some DML queries.

```
root@spqr_client_1_1:/# connect.sh
psql (13.4 (Ubuntu 13.4-1.pgdg20.04+1), server lolkekcheburek)
Type "help" for help.

db1=?> \q
root@spqr_client_1_1:/#
```
