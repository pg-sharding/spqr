
SELECT __spqr__console_execute('CREATE REFERENCE RELATION ref_2pc');

CREATE TABLE ref_2pc(i INT);

SET __spqr__engine_v2 TO true;
SET __spqr__commit_strategy TO 2pc;

BEGIN;
INSERT INTO ref_2pc (i) VALUES (1);
COMMIT;

SELECT __spqr__console_execute('show two_phase_tx (status)');

-- no need 2pc for rollback;
BEGIN;
INSERT INTO ref_2pc (i) VALUES (1);
ROLLBACK;

SELECT __spqr__console_execute('show two_phase_tx (status)');

DROP TABLE ref_2pc;

SELECT __spqr__console_execute('DROP DISTRIBUTION ALL CASCADE');
