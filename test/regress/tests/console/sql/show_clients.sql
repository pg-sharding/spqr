SHOW clients (client_id, user, dbname, server_id, router_address, is_alive);
SHOW clients (client_id, user, dbname, server_id, router_address, is_alive) WHERE server_id = spqr_shard_1:6432;
SHOW clients (client_id, user, dbname, server_id, router_address, is_alive) WHERE server_id = nonexistent:6432 OR dbname = regress;