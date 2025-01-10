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


-- test transaction support for route-local variables;

SET __spqr__distribution = 'ds1';

SHOW __spqr__distribution;

SET __spqr__sharding_key = 1;
SHOW __spqr__sharding_key;

SELECT * FROM test;

SET __spqr__sharding_key = 12;
SELECT * FROM test;

BEGIN;

SET __spqr__sharding_key = 1;

SELECT * FROM test;

ROLLBACK;

-- should return to previous value, so select from second shard
SELECT * FROM test;

RESET __spqr__sharding_key;

\c regress

-- cleanup test relation to avoid confusion
TRUNCATE test;

COPY test (id, age) FROM STDIN /* __spqr__allow_multishard: true */;
1	1
5	5
10	10
15	15
20	20
25	25
\.

-- SELECT here with order to avoid flaps

SELECT * FROM test ORDER BY id /* __spqr__execute_on: sh1 */;
SELECT * FROM test ORDER BY id /* __spqr__execute_on: sh2 */;

SET __spqr__execute_on TO sh1;

SHOW __spqr__execute_on;

SELECT * FROM test ORDER BY id;

SET __spqr__execute_on TO sh2;

SHOW __spqr__execute_on;

SELECT * FROM test ORDER BY id;

SET __spqr__execute_on TO sh1;

-- overrides prev set
SELECT * FROM test ORDER BY id /* __spqr__execute_on: sh2 */;

-- After stmt select from sh1, not sh2
SELECT * FROM test ORDER BY id;

SHOW __spqr__scatter_query; -- error
SHOW __spqr__default_route_behaviour;

RESET __spqr__execute_on;
SHOW __spqr__execute_on;

SET __spqr__default_route_behaviour to 'BLOCK';

SELECT 1 FROM test WHERE id IN (5, 25) /* __spqr__scatter_query: true, __spqr__default_route_behaviour: allow */;

SET __spqr__default_route_behaviour to 'ALLOW';
SHOW __spqr__default_route_behaviour;

SELECT 1 FROM test WHERE id IN (5, 25) /* __spqr__scatter_query: true */;

-- restart session, reset all params
\c regress

DROP TABLE test;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;
