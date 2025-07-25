\c spqr-console

CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
CREATE KEY RANGE krid2 FROM 101 ROUTE TO sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid1 FROM 1 ROUTE TO sh1 FOR DISTRIBUTION ds1;
ALTER DISTRIBUTION ds1 ATTACH RELATION tsa_test DISTRIBUTION KEY id;

\c regress
CREATE TABLE tsa_test (id int);
INSERT INTO tsa_test (id) VALUES (22);

-- you could specify target-session-attrs anywhere in your query
SELECT pg_is_in_recovery() /* target-session-attrs: read-write */ , id FROM tsa_test WHERE id = 22;
/* target-session-attrs: read-write */ SELECT pg_is_in_recovery(), id FROM tsa_test WHERE id = 22;
SELECT pg_is_in_recovery(), id FROM tsa_test WHERE id = 22 /* target-session-attrs: read-write */;

-- read-only is also supported but there is no high availability cluster in our tests yet, so it returns error
SELECT pg_is_in_recovery() /* target-session-attrs: read-only */ , id FROM tsa_test WHERE id = 22;
SELECT NOT pg_is_in_recovery() /* target-session-attrs: read-only */ , id FROM tsa_test WHERE id = 22;

SHOW __spqr__target_session_attrs;
SET __spqr__target_session_attrs TO 'prefer-standby';
SELECT pg_is_in_recovery();
SELECT NOT pg_is_in_recovery();
SHOW __spqr__target_session_attrs;

SET __spqr__execute_on TO sh1;

SET __spqr__engine_v2 TO true;

select (select extract(epoch from TIMESTAMP '2024-12-09T21:05:00' AT TIME ZONE 'UTC-8')::integer) zz;

SHOW __spqr__target_session_attrs;

select (select /* target-session-attrs: prefer-standby */ extract(epoch from TIMESTAMP '2024-12-09T21:05:00' AT TIME ZONE 'UTC-8')::integer) zz;

SET __spqr__target_session_attrs TO 'read-only';
SHOW __spqr__target_session_attrs;

SELECT TO_TIMESTAMP(
    '2017-03-31 9:30:20',
    'YYYY-MM-DD HH:MI:SS'
);

SELECT /* __spqr__target_session_attrs: read-only */TO_TIMESTAMP(
    '2017-03-31 9:30:20',
    'YYYY-MM-DD HH:MI:SS'
);

SHOW __spqr__target_session_attrs;

SET __spqr__target_session_attrs TO 'prefer-standby';
SHOW __spqr__target_session_attrs;

BEGIN;

SET __spqr__target_session_attrs TO 'read-write';
SHOW __spqr__target_session_attrs;

ROLLBACK;

SHOW __spqr__target_session_attrs;

SELECT TO_TIMESTAMP(
    '2017-03-31 9:30:20',
    'YYYY-MM-DD HH:MI:SS'
);

SELECT /* __spqr__target_session_attrs: read-only */TO_TIMESTAMP(
    '2017-03-31 9:30:20',
    'YYYY-MM-DD HH:MI:SS'
);

SHOW __spqr__target_session_attrs;

RESET __spqr__target_session_attrs;

SHOW __spqr__target_session_attrs;

BEGIN READ ONLY;

SHOW __spqr__target_session_attrs;

COMMIT;

SHOW __spqr__target_session_attrs;

RESET __spqr__target_session_attrs;

DROP TABLE tsa_test;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;
