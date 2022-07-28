# Shard

There two types of shards in SPQR:

- **Data shard** is a horizontal partition of data. Each data shard is held on a separate PostgreSQL cluster.

- **World shard** is *a thing* that will have to be able to execute queries that cannot be executed on one shard. For example, `count(*)`, some complex `SELECT` or even `JOIN` queries. We hope that our world shard will be compatible with [Citus](https://github.com/citusdata/citus). For now, world shard has only a mock implementation:

```
➜ psql "host=localhost port=6432 dbname=postgres sslmode=disable"
psql (14devel, server lolkekcheburek)
Type "help" for help.

postgres=?> select  * from x ;
ROUTER NOTICE: you are receiving messagwe from mock world shard
 worldmock
-----------
 row1
(1 row)

postgres=?>
```
