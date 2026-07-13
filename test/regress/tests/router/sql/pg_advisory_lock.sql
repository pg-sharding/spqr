
\set VERBOSITY verbose

SET __spqr__advisory_lock_behaviour TO SCATTER;
SHOW __spqr__advisory_lock_behaviour;

SET __spqr__engine_v2 TO off;

SELECT pg_advisory_lock(11);
SELECT pg_advisory_xact_lock(11);

SET __spqr__advisory_lock_behaviour TO BLOCK;
SHOW __spqr__advisory_lock_behaviour;

SELECT pg_advisory_lock(11);
SELECT pg_advisory_xact_lock(11);

SET __spqr__engine_v2 TO on;

SELECT pg_advisory_lock(11);
SELECT pg_advisory_xact_lock(11);
