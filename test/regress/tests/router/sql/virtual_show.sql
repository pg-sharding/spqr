

SHOW __spqr__distribution;
SHOW __spqr__default_route_behaviour;
SHOW __spqr__auto_distribution;
SHOW __spqr__distribution_key;
SHOW __spqr__sharding_key;
SHOW __spqr__scatter_query;
SHOW __spqr__reply_notice;
SHOW __spqr__maintain_params;
SHOW __spqr__execute_on;
SHOW __spqr__engine_v2;
SHOW __spqr__commit_strategy;
SHOW __spqr__target_session_attrs;
SHOW target_session_attrs;


SET __spqr__engine_v2 TO false;
SHOW __spqr__engine_v2;
SET __spqr__engine_v2 TO ok;
SHOW __spqr__engine_v2;
SHOW __spqr__.engine_v2;

BEGIN;

SET __spqr__.engine_v2 TO off;
SHOW __spqr__engine_v2;

ROLLBACK;

SHOW __spqr__engine_v2;

SET __spqr__.engine_v2 TO OFF;
SHOW __spqr__.engine_v2;

RESET __spqr__.engine_v2;
SHOW __spqr__engine_v2;