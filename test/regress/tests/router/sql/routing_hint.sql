\c spqr-console

CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
CREATE KEY RANGE krid1 FROM 1 ROUTE TO sh1 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid2 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION ds1;
ALTER DISTRIBUTION ds1 ATTACH RELATION test DISTRIBUTION KEY id;

\c regress

CREATE TABLE test(id int, age int);
-- TODO: specify distribution as well as sharding_key
INSERT INTO test(id, age) VALUES (1210, 16) /*__spqr__sharding_key: 1, __spqr__distribution: ds1  */;
INSERT INTO test(id, age) VALUES (10, 16) /*__spqr__sharding_key: 30, __spqr__distribution: ds1  */;
INSERT INTO test(id, age) VALUES (10, 16) /*__spqr__sharding_key: 3000, __spqr__distribution: ds1  */;



-- test tranasction support for route-local variables;

SET __spqr__distribution = 'ds1';

SET __spqr__sharding_key = 1;
SELECT * FROM test;

SET __spqr__sharding_key = 12;
SELECT * FROM test;

BEGIN;

SET __spqr__sharding_key = 1;

SELECT * FROM test;

ROLLBACK;


-- should return to previous value, so select from first shard
SELECT * FROM test;

-- restart session, reset all params
\c regress

DROP TABLE test;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;
