[![Go](https://github.com/pg-sharding/spqr/actions/workflows/build.yaml/badge.svg)](https://github.com/pg-sharding/spqr/actions/workflows/build.yaml)
[![Go](https://github.com/pg-sharding/spqr/actions/workflows/tests.yaml/badge.svg)](https://github.com/pg-sharding/spqr/actions/workflows/tests.yaml)
![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/pg-sharding/spqr)
![Go Report](https://goreportcard.com/badge/github.com/pg-sharding/spqr)
![Lines of code](https://img.shields.io/tokei/lines/github/pg-sharding/spqr)

# Stateless Postgres Query Router

## Overview
SPQR is a liquid OLTP sharding for PostgreSQL.

PostgreSQL is awesome, but it's hard to manage single database with some terabytes of data and 10<sup>5</sup>+ queries per second. Existing sharding solutions focus on analytical workloads. Also most of solutions neglect pains of monolith<->sharded transitions. That's why [Data Platform](https://cloud.yandex.com/en-ru/services#data-platform) team of Yandex.Cloud designed SPQR.

### Core requirements that formed SPQR design
1. Using highly available clusters as building blocks for sharding installations. These clusters can be based on Patroni, Stolon, Managed PostgreSQL or any other HA solution over vanilla Postgres. Physical quorum-based PostgreSQL HA solutions are battle proven and we are reusing all their benefits.
2. Zero downtime for conversion from monolith to sharded cluster and vice versa. Your existing database if first shard o horizontal scaling. And if at some point you want to go back - you do not need to restore from a backup.
3. SPQR dev environment should be installable on developers laptop or Raspberry Pi in minutes, not hours in a datacenter.
4. SPQR is optimised for single-shard OLTP queries.
5. Data migration between shards aims to proportionally balance workload across shards. Migrations must cause as little locking impact as possibly by reducing range size. Liquid migrations should allow transferring between clouds. But in this case high latencies are inevitable.

### Why stateless?
There are some good sharding solutions relying on Postgres codebase for routing. This is reliable and maintainable design decision. SQL grammar is always compatible between same versions of Postgres.

Postgres, as any DBMS, solves hard problem of managing state. And the most important state is system catalog - metadata, data about your data. Postgres allows you to see a snapshot of structure of your data in the past. To make performance of system catalog acceptable it employs sophisticated system of caches with rather tricky invalidations.

At the beginning of our jorney to sharding solutions we tried to implement FDW-based sharding and custom node based sharding.
Both catalog and caches are redundant for task of query routing. At the time of query routing Postgres core will check that colum data types poses necessary casts, support functions, operators etc. Analyze and rewrite phase of query routing made latencies go unreasonably high.

That's why we decided to make query routing that knows about data structure as little as possible. SPQR does not preserve any data besides routing rules.

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


root@spqr_client:/go# connect_adm.sh 


		SQPR router admin console

	Here you can configure your routing rules
------------------------------------------------

	You can find documentation here 
https://github.com/pg-sharding/spqr/tree/master/doc/router


psql (13.4 (Debian 13.4-4.pgdg110+1), server console)
Type "help" for help.

db1=?> 
db1=?> 
db1=?> 
db1=?> 
db1=?> 
db1=?> create sharding column w_id;
                   fortune                    
----------------------------------------------
 created sharding column w_id, err %!w(<nil>)
(1 row)

db1=?> add key range 1 10 sh1 krid1;
                      fortune                      
---------------------------------------------------
 created key range from [49] to [49 48], err <nil>
(1 row)

db1=?> add key range 11 20 sh2 krid2;
                       fortune                        
------------------------------------------------------
 created key range from [49 49] to [50 48], err <nil>
(1 row)

db1=?> 
\q
root@spqr_client:/go# connect.sh 
psql (13.4 (Debian 13.4-4.pgdg110+1), server lolkekcheburek)
Type "help" for help.

db1=?> create table x (w_id int);
ROUTER NOTICE: process Frontend for user user1 db1
ROUTER NOTICE: rerouting your connection
ROUTER NOTICE: matched shard routes [{{sh2 true} {[] []  }} {{sh1 true} {[] []  }}]
ROUTER NOTICE: adding shard sh2
ROUTER NOTICE: adding shard sh1
CREATE TABLE
db1=?> select * from x where w_id <= 10; 
ROUTER NOTICE: rerouting your connection
ROUTER NOTICE: matched shard routes [{{sh1 true} {[49] [49 48] sh1 krid1}}]
ROUTER NOTICE: initialize single shard server conn
ROUTER NOTICE: adding shard sh1
 w_id 
------
(0 rows)

db1=?> insert into x (w_id) values(1);
ROUTER NOTICE: rerouting your connection
ROUTER NOTICE: matched shard routes [{{sh1 true} {[49] [49 48] sh1 krid1}}]
ROUTER NOTICE: initialize single shard server conn
ROUTER NOTICE: adding shard sh1
INSERT 0 1
db1=?> select * from x where w_id <= 10;
ROUTER NOTICE: rerouting your connection
ROUTER NOTICE: matched shard routes [{{sh1 true} {[49] [49 48] sh1 krid1}}]
ROUTER NOTICE: initialize single shard server conn
ROUTER NOTICE: adding shard sh1
 w_id 
------
    1
(1 row)

db1=?> insert into x (w_id) values(11);
ROUTER NOTICE: rerouting your connection
ROUTER NOTICE: matched shard routes [{{sh2 true} {[49 49] [50 48] sh2 krid2}}]
ROUTER NOTICE: initialize single shard server conn
ROUTER NOTICE: adding shard sh2
INSERT 0 1
db1=?> select * from x where w_id <= 10;
ROUTER NOTICE: rerouting your connection
ROUTER NOTICE: matched shard routes [{{sh1 true} {[49] [49 48] sh1 krid1}}]
ROUTER NOTICE: initialize single shard server conn
ROUTER NOTICE: adding shard sh1
 w_id 
------
    1
(1 row)

db1=?> select * from x where w_id <= 20;
ROUTER NOTICE: rerouting your connection
ROUTER NOTICE: matched shard routes [{{sh2 true} {[49 49] [50 48] sh2 krid2}}]
ROUTER NOTICE: initialize single shard server conn
ROUTER NOTICE: adding shard sh2
 w_id 
------
   11
(1 row)

db1=?> select hello;
ROUTER NOTICE: rerouting your connection
ROUTER NOTICE: failed to match shard
ROUTER NOTICE: initialize single shard server conn
ROUTER NOTICE: adding shard w1
ROUTER NOTICE: you are receiving message from mock world shard
 worldmock 
-----------
 row1
(1 row)

db1=?> 



```
