\c spqr-console
CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
CREATE KEY RANGE krid2 FROM 101 ROUTE TO sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid1 FROM 1 ROUTE TO sh1 FOR DISTRIBUTION ds1;
ALTER DISTRIBUTION ds1 ATTACH RELATION xxtest_sw DISTRIBUTION KEY id;

\c regress

SET __spqr__maintain_params To true;
SET __spqr__reply_notice TO false;

SHOW __spqr__maintain_params;
SHOW __spqr__reply_notice;

CREATE TABLE xxtest_sw (id int);

SET application_name = 'a1';

BEGIN;
SET application_name = 'a2';
SHOW application_name;
ROLLBACK;

SHOW application_name;

BEGIN;
SET application_name = 'a3';
SHOW application_name;
COMMIT;

SHOW application_name;

SET __spqr__reply_notice TO true;

INSERT INTO xxtest_sw (id) VALUES(1), (2), (3);

SELECT * from xxtest_sw WHERE id <= 2;

SET search_path to 'error';

-- should fail
SELECT * from xxtest_sw WHERE id <= 2;

SET search_path to 'public';

-- should success
SELECT * from xxtest_sw WHERE id <= 2;

DROP TABLE xxtest_sw;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
