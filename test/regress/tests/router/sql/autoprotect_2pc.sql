
SELECT __spqr__console_execute('CREATE REFERENCE RELATION ref_2pc');

CREATE TABLE ref_2pc(i INT);

SET __spqr__engine_v2 TO true;

select __spqr__clear_2pc_data();

SELECT __spqr__set_next_2pc_gid('auto_zzz1');

BEGIN;
INSERT INTO ref_2pc (i) VALUES (1);
COMMIT;

SELECT __spqr__set_next_2pc_gid('auto_zzz2');
set __spqr__allow_autoprotect_2pc to true;

BEGIN;
INSERT INTO ref_2pc (i) VALUES (1);
COMMIT;

SELECT __spqr__set_next_2pc_gid('auto_zzz3');

INSERT INTO ref_2pc (i) VALUES (1);

SELECT __spqr__set_next_2pc_gid('auto_zzz4');

UPDATE ref_2pc SET i = i + 1;

SELECT __spqr__set_next_2pc_gid('auto_zzz5');

DELETE FROM ref_2pc;

SELECT __spqr__console_execute('show two_phase_tx (gid, status)');

DROP TABLE ref_2pc;

SELECT __spqr__console_execute('DROP DISTRIBUTION ALL CASCADE');
