SHOW clients;
SHOW clients WHERE server_id = spqr_shard_1:6432;
SHOW clients WHERE server_id = nonexistent:6432 OR dbname = regress;