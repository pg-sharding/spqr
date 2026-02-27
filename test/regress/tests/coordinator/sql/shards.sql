REGISTER ROUTER r1 ADDRESS "[regress_router]:7000";

DROP SHARD sh1;
SHOW SHARDS;
CREATE SHARD sh1 OPTIONS (HOST 'spqr_shard_1:6432', HOST 'spqr_shard_1_replica:6432');
SHOW SHARDS;
ALTER SHARD sh1 OPTIONS (SSLMODE 'require');
SHOW SHARDS;
ALTER SHARD sh1 OPTIONS (DROP HOST 'spqr_shard_1_replica:6432');
SHOW SHARDS;

<<<<<<< HEAD
UNREGISTER ROUTER ALL;
=======
ALTER SHARD shard1 OPTIONS (dbname db1);
ALTER SHARD shard2 HOSTS 'localhost3:6432';

SHOW shards;

DROP SHARD shard1;
DROP SHARD shard2;
>>>>>>> 37e567f1 (regress tests)
