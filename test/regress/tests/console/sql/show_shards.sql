DROP SHARD sh1;
DROP SHARD sh2;
DROP SHARD sh3;
DROP SHARD sh4;

CREATE SHARD sh1 WITH HOSTS spqr_shard_1:6432,spqr_shard_1_replica:6432;
CREATE SHARD sh2 WITH HOSTS spqr_shard_1:6432,spqr_shard_1_replica:6432;
CREATE SHARD sh3 WITH HOSTS spqr_shard_1:6432,spqr_shard_1_replica:6432;
CREATE SHARD sh4 WITH HOSTS spqr_shard_1:6432,spqr_shard_1_replica:6432;

CREATE SHARD sh1 WITH HOSTS localhost:6432;

SHOW shards;
SHOW hosts;

CREATE SHARD sh5 WITH HOSTS e:6432;

SHOW shards;
SHOW hosts;

DROP SHARD sh5;

SHOW shards;
SHOW hosts;