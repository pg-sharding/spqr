\set VERBOSITY verbose

SET __spqr__advisory_lock_behaviour TO SCATTER;
SHOW __spqr__advisory_lock_behaviour;

SET __spqr__engine_v2 TO off;

SELECT pg_try_advisory_xact_lock(11);

SET __spqr__advisory_lock_behaviour TO BLOCK;
SHOW __spqr__advisory_lock_behaviour;

SELECT pg_try_advisory_xact_lock(11);

SET __spqr__engine_v2 TO on;

SELECT pg_try_advisory_xact_lock(11);

SET __spqr__advisory_lock_behaviour TO SCATTER;

BEGIN;
SELECT pg_try_advisory_xact_lock(12);
ROLLBACK;

SHOW __spqr__session_connections_pin;