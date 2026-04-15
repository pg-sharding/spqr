
SELECT __spqr__console_execute('CREATE DISTRIBUTION d (int)');
SELECT __spqr__console_execute('ALTER DISTRIBUTION d ADD DEFAULT SHARD sh1;');
SELECT __spqr__console_execute('CREATE RELATION d_p_exec(i)');
SELECT __spqr__console_execute('CREATE REFERENCE RELATION ref_p_exec');


CREATE TABLE ref_p_exec(i INT);
CREATE TABLE d_p_exec(i INT);

SET __spqr__engine_v2 TO true;
SET __spqr__commit_strategy TO 1pc;

PREPARE p1 AS INSERT INTO ref_p_exec (i) VALUES (1);

BEGIN;
EXECUTE p1;
COMMIT;

SELECT __spqr__ctid('ref_p_exec');

BEGIN;
EXECUTE p1;
ROLLBACK;

SELECT __spqr__ctid('ref_p_exec');

INSERT INTO d_p_exec(i) VALUES(1);

PREPARE p5 AS SELECT $1 FROM d_p_exec;

EXECUTE p5 ('aaaa');

DROP TABLE d_p_exec;
DROP TABLE ref_p_exec;

SELECT __spqr__console_execute('DROP DISTRIBUTION ALL CASCADE');
