# Router

Consist from 3 parts:
- a query router itself
- admin console, where you can set key ranges and create 
- grpc api

# Config

log_level, can be TODO
host, the router and its apps will be run on this host
router_port, the router port
admin_console_port, the admin console port
grpc_api_port, the API port
world_shard_address, TODO
world_shard_fallback, TODO
auto_conf, a configuration file with the same format as spqr config file, but which is managed by SPQR itself
init_sql, a path to a sql commands, that will be run on the startup of the router.
router_mode, mode in which router will be run. Can be LOCAL, works like an usual connection pooler with 1 shard, and PROXY, for work with many shards.
jaeger_url, TODO
frontend_tls, TODO
frontend_rules, TODO
backend_rules, TODO
shards, TODO
