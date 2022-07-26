# Router

Consist from 3 parts:
- a query router itself
- admin console, where you can set key ranges and create 
- grpc api

![Router schema](router.jpg "Router")

# Config

| **Name**               | **Description**                                                                                                                                                                               |
| ---------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
|  **general**           |                                                                                                                                                                                               |
| `log_level`            | can be `FATAL`, `ERROR`, `INFO`, `WARNING` and `DEBUG`.                                                                                                                                                 |
|                        |                                                                                                                                                                                               |
| `host`                 | the router and its apps will be run on this host                                                                                                                                              |
| `router_port`          | the router port                                                                                                                                                                               |
| `admin_console_port`   | the admin console port                                                                                                                                                                        |
| `grpc_api_port`        | the API port                                                                                                                                                                                  |
|                        |                                                                                                                                                                                               |
| `world_shard_address`  | `host:port` of the [world shard](worldshard.md)                                                                                                                                               |
| `world_shard_fallback` | can be true or false. If false, then router will raise an error when query will be impossible to send to particular shard. If true, then router will route unrouted query to the world shard. |
|                        |                                                                                                                                                                                               |
| `auto_conf`            | a configuration file with the same format as spqr config file, but which is managed by SPQR itself                                                                                            |
| `init_sql`             | a path to a sql commands, that will be run on the startup of the router.                                                                                                                      |
| `router_mode`          | mode in which router will be run. Can be LOCAL and PROXY. In local mode spqr works like an usual connection pooler with one shard, in proxy mode works with many shards.                      |
| `jaeger_url`           | TODO                                                                                                                                                                                          |
|                        |                                                                                                                                                                                               |
| `frontend_tls`         | TODO                                                                                                                                                                                          |
| `frontend_tls`         | TODO                                                                                                                                                                                          |
|                        |                                                                                                                                                                                               |
| `frontend_rules`       | TODO                                                                                                                                                                                          |
|                        |                                                                                                                                                                                               |
| `backend_rules`        | TODO                                                                                                                                                                                          |
|                        |                                                                                                                                                                                               |
| `shards`               | TODO                                                                                                                                                                                          |
