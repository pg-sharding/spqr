\c spqr-console

CREATE DISTRIBUTION ds1 COLUMN TYPES integer;

-- test both ways of ref relation crete syntax
CREATE REFERENCE TABLE test_ref_rel;

-- test schema-qualified creation
CREATE REFERENCE TABLE sh1.test_ref_rel_rel;

-- partial ref relation test
CREATE REFERENCE RELATION test_ref_rel_part ON sh1, sh3;

CREATE DISTRIBUTED RELATION test_distr_ref_rel DISTRIBUTION KEY id IN ds1;
CREATE KEY RANGE kr4 FROM 300 ROUTE TO sh4 FOR DISTRIBUTION ds1;
CREATE KEY RANGE kr3 FROM 200 ROUTE TO sh3 FOR DISTRIBUTION ds1;
CREATE KEY RANGE kr2 FROM 100 ROUTE TO sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;

\c regress

CREATE TABLE test_ref_rel(i int, j int);

CREATE SCHEMA sh1;
CREATE TABLE sh1.test_ref_rel_rel(i int, j int);

CREATE TABLE test_distr_ref_rel(id int, val int);

CREATE TABLE test_ref_rel_part(i int, j int);

COPY test_ref_rel FROM STDIN;
1	2
2	3
3	4
4	5
\.

COPY sh1.test_ref_rel_rel FROM STDIN;
1	2
2	3
3	4
4	5
\.

COPY test_ref_rel_part FROM STDIN;
1	2
2	3
3	4
4	5
\.

COPY test_distr_ref_rel(id, val) FROM STDIN WITH DELIMITER '|';
1|2
2|3
4|5
50|51
100|101
101|102
150|151
199|200
200|201
201|202
250|251
299|300
300|301
350|351
\.

set __spqr__default_route_behaviour to allow;

SELECT * FROM test_ref_rel ORDER BY i, j /*__spqr__execute_on: sh1 */;

set __spqr__default_route_behaviour to block;

SELECT * FROM test_ref_rel ORDER BY i, j /*__spqr__execute_on: sh2 */;

INSERT INTO test_ref_rel VALUES(1) /* __spqr__engine_v2: true */;

WITH data(x,z) AS (VALUES(1,3)) INSERT INTO test_ref_rel SELECT * FROM data;

INSERT INTO test_ref_rel SELECT 1 /* __spqr__engine_v2: true */;
INSERT INTO sh1.test_ref_rel_rel SELECT 1 /* __spqr__engine_v2: true */;

SELECT * FROM test_ref_rel ORDER BY i, j /*__spqr__execute_on: sh1 */;
SELECT * FROM test_ref_rel ORDER BY i, j /*__spqr__execute_on: sh2 */;
SELECT * FROM test_ref_rel ORDER BY i, j /*__spqr__execute_on: sh3 */;
SELECT * FROM test_ref_rel ORDER BY i, j /*__spqr__execute_on: sh4 */;


SELECT * FROM sh1.test_ref_rel_rel ORDER BY i, j /*__spqr__execute_on: sh1 */;
SELECT * FROM sh1.test_ref_rel_rel ORDER BY i, j /*__spqr__execute_on: sh2 */;
SELECT * FROM sh1.test_ref_rel_rel ORDER BY i, j /*__spqr__execute_on: sh3 */;
SELECT * FROM sh1.test_ref_rel_rel ORDER BY i, j /*__spqr__execute_on: sh4 */;

UPDATE test_ref_rel SET i = i + 1 /* __spqr__engine_v2: true */;

UPDATE test_ref_rel SET i = - i WHERE i IN (3, 4) /* __spqr__engine_v2: true */;

SELECT * FROM test_ref_rel ORDER BY i, j /*__spqr__execute_on: sh1 */;
SELECT * FROM test_ref_rel ORDER BY i, j /*__spqr__execute_on: sh2 */;
SELECT * FROM test_ref_rel ORDER BY i, j /*__spqr__execute_on: sh3 */;
SELECT * FROM test_ref_rel ORDER BY i, j /*__spqr__execute_on: sh4 */;

DELETE FROM test_ref_rel WHERE i = 2 /* __spqr__engine_v2: true */;

-- check data
SELECT * FROM test_ref_rel ORDER BY i, j /*__spqr__execute_on: sh1 */;
SELECT * FROM test_ref_rel ORDER BY i, j /*__spqr__execute_on: sh2 */;
SELECT * FROM test_ref_rel ORDER BY i, j /*__spqr__execute_on: sh3 */;
SELECT * FROM test_ref_rel ORDER BY i, j /*__spqr__execute_on: sh4 */;

INSERT INTO test_ref_rel_part VALUES(1) /* __spqr__engine_v2: true */;
INSERT INTO test_ref_rel_part SELECT 1 /* __spqr__engine_v2: true */;

-- check data on partially distributed reference relation
SELECT * FROM test_ref_rel_part ORDER BY i, j /*__spqr__execute_on: sh1 */;
SELECT * FROM test_ref_rel_part ORDER BY i, j /*__spqr__execute_on: sh2 */;
SELECT * FROM test_ref_rel_part ORDER BY i, j /*__spqr__execute_on: sh3 */;
SELECT * FROM test_ref_rel_part ORDER BY i, j /*__spqr__execute_on: sh4 */;

/* Check execution for returning clause */

TRUNCATE test_ref_rel;

INSERT INTO test_ref_rel VALUES(1,3),(2,4),(3,-1) RETURNING *;

-- check data on partially distributed reference relation
SELECT * FROM test_ref_rel ORDER BY i, j /*__spqr__execute_on: sh1 */;
SELECT * FROM test_ref_rel ORDER BY i, j /*__spqr__execute_on: sh2 */;
SELECT * FROM test_ref_rel ORDER BY i, j /*__spqr__execute_on: sh3 */;
SELECT * FROM test_ref_rel ORDER BY i, j /*__spqr__execute_on: sh4 */;


-- Check routing for reference relation JOIN distributed relation
SELECT FROM test_distr_ref_rel a JOIN test_ref_rel b ON TRUE WHERE a.id = 333;
SELECT FROM test_distr_ref_rel a, test_ref_rel b WHERE a.id = 133;

SELECT FROM test_distr_ref_rel a, test_ref_rel_part b WHERE a.id = 33;
SELECT FROM test_distr_ref_rel a, test_ref_rel_part b WHERE a.id = 233;

SET __spqr__preferred_engine TO v2;
SELECT __spqr__show('reference_relations');
SET __spqr__preferred_engine TO '';

DROP TABLE test_ref_rel;
DROP TABLE sh1.test_ref_rel_rel;
DROP SCHEMA sh1;
DROP TABLE test_ref_rel_part;
DROP TABLE test_distr_ref_rel;


\c spqr-console
SHOW reference_relations;
DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;

