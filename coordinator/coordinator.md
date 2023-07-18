Coordinator provides syncronisation between routers in multi-router installation. 

## Interface

Coordinator supports PostgreSQL simple(wire) procol and own SQL-like interface.


Suppose coordinator is up and running on [localhost]:7432

You can connect to coordinator via 

```
psql "host=localhost port=7002 dbname=spqr-console"
```

Then, run `SHOW routers;`. Coordinator will reply with list of knows router in current spqr installation
