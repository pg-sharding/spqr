
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

SELECT __spqr__console_execute('DROP DISTRIBUTION ALL CASCADE');
