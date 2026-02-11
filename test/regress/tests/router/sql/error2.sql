\c spqr-console

CREATE DISTRIBUTION ds1 COLUMN TYPES INTEGER;

ALTER DISTRIBUTION ds1 ATTACH RELATION tt DISTRIBUTION KEY id;

CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid2 FROM 50 ROUTE TO sh2 FOR DISTRIBUTION ds1;

\c regress

-- test table does not exist
INSERT INTO tt (id, data) VALUES (2, 'valid');

-- create table after error
CREATE TABLE tt(id INT PRIMARY KEY, data TEXT);

-- insert initial data
INSERT INTO tt (id, data) VALUES (1, 'first');
INSERT INTO tt (id, data) VALUES (51, 'second');

-- test unique constraint violation on shard
BEGIN;
INSERT INTO tt (id, data) VALUES (2, 'valid');
-- this should cause a unique constraint violation error from shard
INSERT INTO tt (id, data) VALUES (1, 'duplicate');
-- after error, transaction should be aborted - this should fail with SPQRU
INSERT INTO tt (id, data) VALUES (3, 'should_fail');
ROLLBACK;

-- test that table is still accessible after rollback
SELECT COUNT(*) FROM tt;

-- test cross-shard transaction with error
BEGIN;
INSERT INTO tt (id, data) VALUES (10, 'shard1');
INSERT INTO tt (id, data) VALUES (60, 'shard2');
-- violate constraint on different shard
INSERT INTO tt (id, data) VALUES (51, 'duplicate_shard2');
-- should fail with transaction aborted error
SELECT * FROM tt WHERE id = 10;
ROLLBACK;


-- test ending tx block with COMMIT
BEGIN;
SELECT 1/0 /* __spqr__.execute_on: sh1 */;
COMMIT;

DROP TABLE tt;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
