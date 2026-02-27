ADD SHARD shard1 WITH HOSTS 'localhost:6432' OPTIONS (user user1);
ADD SHARD shard2 WITH HOSTS 'localhost1:6432', 'localhost2:6432' OPTIONS (dbname db1);

SHOW shards;

ALTER SHARD shard1 OPTIONS (dbname db1);
ALTER SHARD shard2 WITH HOSTS 'localhost3:6432' OPTIONS (password password);

SHOW shards;

DROP SHARDS ALL;
