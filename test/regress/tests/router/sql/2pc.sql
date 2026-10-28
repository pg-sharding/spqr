
SELECT __spqr__console_execute('CREATE REFERENCE RELATION ref_2pc');

CREATE TABLE ref_2pc(i INT);

SET __spqr__engine_v2 TO true;
SET __spqr__commit_strategy TO 2pc;

SELECT __spqr__set_next_2pc_gid('zzz1');

BEGIN;
INSERT INTO ref_2pc (i) VALUES (1);
COMMIT;

SELECT __spqr__console_execute('show two_phase_tx (gid, status)');

SELECT __spqr__set_next_2pc_gid('zzz2');

-- no need 2pc for rollback;
BEGIN;
INSERT INTO ref_2pc (i) VALUES (1);
ROLLBACK;

SELECT __spqr__console_execute('show two_phase_tx (gid, status)');

DROP TABLE ref_2pc;

SELECT __spqr__console_execute('show two_phase_tx (gid, status)');

SELECT __spqr__set_next_2pc_gid('zzz3');

BEGIN;
CREATE TEMP TABLE xzz() /* __spqr__scatter_query: true */;
-- will fail, we have temporal objects in session.
COMMIT;
ROLLBACK;

SELECT __spqr__console_execute('show two_phase_tx (gid, status)');

/* eager cleanup 2pc: on success 2PC, metadata should be cleaned up */
SET __spqr__eager_cleanup_2pc TO true;
SELECT __spqr__set_next_2pc_gid('zzz4');

BEGIN;
SELECT 1+1 /* __spqr__scatter_query: true */ ;
COMMIT;

SELECT __spqr__console_execute('show two_phase_tx (gid, status)');

SET __spqr__eager_cleanup_2pc TO false;

/* __spqr__execute_on: sh1 */ SELECT * FROM spqr_metadata.spqr_distributed_relations;

SELECT __spqr__console_execute('DROP DISTRIBUTION ALL CASCADE');
