\c spqr-console
CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
CREATE KEY RANGE krid1 FROM 1 ROUTE TO sh1 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid2 FROM 101 ROUTE TO sh2 FOR DISTRIBUTION ds1;
ALTER DISTRIBUTION ds1 ATTACH RELATION transactions_test DISTRIBUTION KEY id;

\c regress

CREATE TABLE transactions_test (id int);

-- check that rollbacked changes do no apply
BEGIN;
SELECT * FROM transactions_test WHERE id = 1;;
INSERT INTO transactions_test (id) VALUES (1);
SELECT * FROM transactions_test WHERE id = 1;;
ROLLBACK;

SELECT * FROM transactions_test WHERE id = 1;;

-- check that commited changes present
BEGIN;
SELECT * FROM transactions_test WHERE id = 1;;
INSERT INTO transactions_test (id) VALUES (1);
SELECT * FROM transactions_test WHERE id = 1;;
COMMIT;

SELECT * FROM transactions_test WHERE id = 1;;

DROP TABLE transactions_test;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;
