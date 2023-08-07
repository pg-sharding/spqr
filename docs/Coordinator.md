# Coordinator

Cordinator provides syncronisation between routers in multi-router installation.
- It blocks key ranges on a shard for modification
- And moves key ranges consistency from one shard to another
- Supports PostgreSQL simple(wire) procol and own SQL-like interface.
- Up and running on [localhost]:7432. You can connect to coordinator via psql:

```
psql "host=localhost port=7002 dbname=spqr-console"
```

Then, run `SHOW routers;`. Coordinator will reply with list of knows router in current spqr installation

## Data Balancing

We considered different options for moving data. The most popular way is to make a copy via logical replication, then delete half of the data on one node and delete another half of the data on the other node. We decided that logical replication does not work well enough yet. Instead, the coordinator makes ε-split - cut off a small part of the data. Since it is small, it all works very quickly.

![ε-split](e-split.png "ε-split")

The load from the moving key ranges measured with [pg_comment_stats](https://github.com/munakoiso/pg_comment_stats) extension.

```
> /* a: 1 c: hmm*/ select 1;
> select comment_keys, query_count, user_time from pgcs_get_stats() limit 1;
-[ RECORD 1 ]+----------------------
comment_keys | {"a": "1"}
query_count  | 1
user_time    | 6.000000000000363e-06
```
