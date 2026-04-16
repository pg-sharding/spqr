DROP SHARD sh1;
DROP SHARD sh2;
DROP SHARD sh3;
DROP SHARD sh4;

CREATE SHARD sh1 OPTIONS (HOST "spqr_shard_1:6432", HOST "spqr_shard_1_replica:6432");
CREATE SHARD sh2 OPTIONS (HOST "spqr_shard_2:6432", HOST "spqr_shard_2_replica:6432");
CREATE SHARD sh3 OPTIONS (HOST "spqr_shard_3:6432", HOST "spqr_shard_3_replica:6432");
CREATE SHARD sh4 OPTIONS (HOST "spqr_shard_4:6432", HOST "spqr_shard_4_replica:6432");

CREATE SHARD sh1 OPTIONS (HOST "localhost:6432");

SHOW shards;
SHOW hosts;

CREATE SHARD sh5 OPTIONS (HOST "127.0.0.1:1");

SHOW shards;
SHOW hosts;

DROP SHARD sh5;

SHOW shards;
SHOW hosts;