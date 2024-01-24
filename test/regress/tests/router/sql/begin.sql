\c spqr-console
CREATE SHARDING RULE t1 COLUMNS id;
CREATE KEY RANGE krid1 FROM 1 ROUTE TO sh1;
CREATE KEY RANGE krid2 FROM 11 ROUTE TO sh2;

\c regress
CREATE TABLE test_beg(id int, age int);
INSERT INTO test_beg(id, age) VALUES (10, 16);
INSERT INTO test_beg(id, age) VALUES (10, 16);
SELECT * FROM test_beg WHERE id=10;

BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SELECT * FROM test_beg WHERE id=10;
INSERT INTO test_beg(id, age) VALUES (10, 16);
SELECT * FROM test_beg WHERE id=10;
ROLLBACK;

BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
SELECT * FROM test_beg WHERE id=10;
INSERT INTO test_beg(id, age) VALUES (10, 16);
SELECT * FROM test_beg WHERE id=10;
ROLLBACK;

BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SELECT * FROM test_beg WHERE id=10;
INSERT INTO test_beg(id, age) VALUES (10, 16);
SELECT * FROM test_beg WHERE id=10;
ROLLBACK;

BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
SELECT * FROM test_beg WHERE id=10;
INSERT INTO test_beg(id, age) VALUES (10, 16);
SELECT * FROM test_beg WHERE id=10;
ROLLBACK;

/* TODO: Different ISOLATION LEVEL have to effect */

BEGIN TRANSACTION READ WRITE;
SELECT * FROM test_beg WHERE id=10;
INSERT INTO test_beg(id, age) VALUES (10, 16);
SELECT * FROM test_beg WHERE id=10;
ROLLBACK;

BEGIN TRANSACTION DEFERRABLE;
SELECT * FROM test_beg WHERE id=10;
INSERT INTO test_beg(id, age) VALUES (10, 16);
SELECT * FROM test_beg WHERE id=10;
ROLLBACK;

BEGIN TRANSACTION NOT DEFERRABLE;
SELECT * FROM test_beg WHERE id=10;
INSERT INTO test_beg(id, age) VALUES (10, 16);
SELECT * FROM test_beg WHERE id=10;
ROLLBACK;

BEGIN TRANSACTION READ ONLY;
SELECT * FROM test_beg WHERE id=10;
INSERT INTO test_beg(id, age) VALUES (10, 16);
ROLLBACK;

DROP TABLE test_beg;

\c spqr-console
DROP DATASPACE ALL CASCADE
DROP SHARDING RULE ALL;
DROP KEY RANGE ALL;