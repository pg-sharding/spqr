# SPQR Proto Specs

The SPQR coordinator uses protocol buffers to configure the metadata of its routers.

When a user configures the topology/data sharding using the router's SQL interface, this information is translated into a proto-gRPC call to the coordinator. The coordinator then issues further proto-gRPC calls to all the routers in the system.