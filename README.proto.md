# SPQR proto specs.


SPQR coordinator uses protocol buffers to configure its routers metadata.

So, when user configures topology/data sharding using router SQL interface, in
spqr-infra and spqr-coordinator setups, this SQL will be translated to proto-grpc coordinator call, which will then issue another proto-grpc calls to all other routers in a system. 