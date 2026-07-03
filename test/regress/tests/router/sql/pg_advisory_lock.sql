

SET __spqr__engine_v2 TO off;

SELECT pg_advisory_lock(11);
SELECT pg_advisory_xact_lock(11);