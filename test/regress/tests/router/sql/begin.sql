\c spqr-console
DROP SHARDING RULE ALL;
DROP KEY RANGE ALL;
ADD SHARDING RULE t1 COLUMNS id;
ADD KEY RANGE krid1 FROM 1 TO 11 ROUTE TO sh1;
ADD KEY RANGE krid2 FROM 11 TO 101 ROUTE TO sh2;

\c regress

CREATE TABLE test_beg(id int, age int);
BEGIN;
INSERT INTO test_beg(id, age) VALUES (10, 16);
INSERT INTO test_beg(id, age) VALUES (10, 16);
INSERT INTO test_beg(id, age) VALUES (10, 16);
ROLLBACK;
SELECT * from test_beg;
BEGIN;
INSERT INTO test_beg(id, age) VALUES (10, 16);
INSERT INTO test_beg(id, age) VALUES (10, 16);
INSERT INTO test_beg(id, age) VALUES (20, 16);
COMMIT;
SELECT * from test_beg;
