[![Go](https://github.com/pg-sharding/spqr/actions/workflows/build.yaml/badge.svg)](https://github.com/pg-sharding/spqr/actions/workflows/build.yaml)
[![Go](https://github.com/pg-sharding/spqr/actions/workflows/tests.yaml/badge.svg)](https://github.com/pg-sharding/spqr/actions/workflows/tests.yaml)
![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/pg-sharding/spqr)
![Go Report](https://goreportcard.com/badge/github.com/pg-sharding/spqr)
![Lines of code](https://img.shields.io/tokei/lines/github/pg-sharding/spqr)

# Stateless Postgres Query Router (SPQR)

SPQR is a liquid OLTP sharding for PostgreSQL.

## Development
How to build:
```
make
make build
```

Try it:
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
