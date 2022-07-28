# Concepts

PostgreSQL is awesome, but it's hard to manage a single database with some terabytes of data and 10<sup>5</sup>+ queries per second. Existing sharding solutions focus on analytical and hybrid workloads (OLAP, HTAP). Moreover, most of those solutions do not provide a simple, painless path for the monolith<->sharded transitions. That's why the [Data Platform](https://cloud.yandex.com/en-ru/services#data-platform) team of Yandex.Cloud designed SPQR.

## Core requirements
1. Use highly available clusters as building blocks for sharding installations. These clusters can be based on Patroni, Stolon, Managed PostgreSQL, or any other HA solution over vanilla Postgres. Physical quorum-based PostgreSQL HA solutions are battle-proven, and we reuse all their benefits.
2. Zero downtime for conversion from monolith to sharded cluster and vice versa. The existing database (monolith) is the initial shard for horizontal scaling. Then we add new nodes and move data to them without downtime, eventually having multiple shards. When needed, we repeat the process, adding more shards – again, without downtime. And if at some point we want to go back to the monolith, we use the same tooling for that, without downtime and without the need to restore from backups.  
3. SPQR dev/test environments should be installable on a developer's laptop or Raspberry Pi in minutes, not hours in a datacenter.
4. SPQR is optimized for single-shard OLTP queries. We aim to have a very low latency overhead (1-2 ms) introduced by SPQR for such queries.
5. Data migration between shards aims to balance the workload across shards proportionally. Migrations must cause as little locking impact as possible. One of the ways to achieve that is to reduce range size. Liquid migrations should allow transferring between clouds. However, in this case, temporary high latencies are inevitable.

## Why stateless?
There are some good sharding solutions relying on the Postgres codebase for routing. This is a reliable and maintainable design decision. One of the obvious benefits of this approach is that the SQL grammar is always compatible between the same versions of Postgres.

Postgres, as any DBMS, solves the hard problem of state management. And the most important state is system catalog - metadata, data about your data. Postgres allows you to see a snapshot of the structure of your data in the past. To make the performance of the system catalog acceptable, it employs a sophisticated system of caches with rather tricky invalidations.

At the beginning of our journey to sharding solutions, we tried to implement FDW-based sharding and custom node based sharding. Eventually, we concluded that both catalog and caches are excessive elements for the task of query routing. At query routing time, Postgres core checks that column data types pose necessary casts, support functions, operators, etc. The "analyze" and "rewrite" phases of the query routing made latencies go unreasonably high.

That's why we decided to build a query routing component that knows about data structure as little as possible. SPQR does not preserve any data besides routing rules.