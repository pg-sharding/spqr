\c spqr-console
CREATE SHARDING RULE t1 COLUMNS id;
CREATE KEY RANGE krid1 FROM 1 ROUTE TO sh1;
CREATE KEY RANGE krid2 FROM 101 ROUTE TO sh2;

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
DROP SHARDING RULE ALL;
DROP KEY RANGE ALL;
