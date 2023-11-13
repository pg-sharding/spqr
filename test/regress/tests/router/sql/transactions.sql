\c spqr-console
ADD SHARDING RULE t1 COLUMNS id;
ADD KEY RANGE krid1 FROM 1 TO 101 ROUTE TO sh1;
ADD KEY RANGE krid2 FROM 101 TO 201 ROUTE TO sh2;

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
DROP KEY RANGE ALL;
DROP SHARDING RULE ALL;
