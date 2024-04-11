# Coordinator

Cordinator provides syncronisation between routers in multi-router installation.
- It blocks key ranges on a shard for modification
- And moves key ranges consistency from one shard to another
- Supports PostgreSQL simple(wire) procol and own SQL-like interface.
- Up and running on [localhost]:7002. You can connect to coordinator via psql:

```
psql "host=localhost port=7002 dbname=spqr-console"
```

Then, run `SHOW routers;`. Coordinator will reply with list of knows router in current spqr installation

It is possible to run coordinator as a separate entity or with router using `with_coordinator` flag

