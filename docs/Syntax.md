# Syntax

Before the start, you need to configure the rules by which the router will decide which of the shards to send each request to.

For this purpose, SPQR has an administrative console. This is an app that works by PostgreSQL protocol and you can connect to it by usual psql. You can find the console port in your config file.

```
➜  psql "host=localhost sslmode=disable user=demo dbname=demo port=7432"
SPQR router admin console
Here you can set up your own routing rules
------------------------------------------------
You can find the documentation here
https://github.com/pg-sharding/spqr/tree/master/docs

psql (14.5 (Homebrew), server console)
Type "help" to get help.

demo=> SHOW shards;
  listing data shards  
-----------------------
 datashard with ID shard1
 datashard with ID shard2
(2 rows)
```

To make all things work, the router needs to know the following:

- What tables do you query
- Which columns in each table should the router search for
- Types of these columns
- Mapping from [range of values] to [shard number]

Let's create a distribution first:

```
➜ psql "host=localhost sslmode=disable user=demo dbname=demo port=7432"
demo=> CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
         add distribution         
----------------------------------
 created distribution with id ds1
(1 row)
```

The next step is to specify a list of tables and columns.

```
demo=> ALTER DISTRIBUTION ds1 ATTACH RELATION orders DISTRIBUTION KEY id;
                 attach table               
--------------------------------------------
 attached relation orders to distribution ds1
(1 row)

demo=> ALTER DISTRIBUTION ds1 ATTACH RELATION item DISTRIBUTION KEY order_id;
                 attach table                 
---------------------------------------------
 attached relation item to distribution ds1
(1 row)
```

And at the end specify a list of ranges: which values to route to which shard. Note: The right bound is infinity if there are no key ranges.

```
CREATE KEY RANGE krid1 FROM 1 ROUTE TO shard1 FOR DISTRIBUTION ds1;
         add key range          
--------------------------------
 created key range with bound 1
(1 row)

CREATE KEY RANGE krid2 FROM 1000 ROUTE TO shard2 FOR DISTRIBUTION ds1;
            add key range          
-----------------------------------
 created key range with bound 1000
(1 row)
``

Here we go!
