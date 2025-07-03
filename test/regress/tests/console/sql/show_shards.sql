SHOW shards;
SHOW hosts;

CREATE SHARD sh1 WITH HOSTS localhost:6432;
CREATE SHARD sh5 WITH HOSTS localhost:6432;

SHOW shards;
SHOW hosts;

DROP SHARD sh5;

SHOW shards;
SHOW hosts;