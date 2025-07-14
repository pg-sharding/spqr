\c spqr-console

-- test both ways of ref relation crete syntax
CREATE REFERENCE TABLE test_ref_rel;

-- partial ref relation test
CREATE REFERENCE RELATION test_ref_rel_part ON sh1, sh3;

\c regress

CREATE TABLE test_ref_rel(i int, j int);

CREATE TABLE test_ref_rel_part(i int, j int);

COPY test_ref_rel FROM STDIN;
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

set __spqr__default_route_behaviour to allow;

SELECT * FROM test_ref_rel ORDER BY i, j /*__spqr__execute_on: sh1 */;

set __spqr__default_route_behaviour to block;

SELECT * FROM test_ref_rel ORDER BY i, j /*__spqr__execute_on: sh2 */;

INSERT INTO test_ref_rel VALUES(1) /* __spqr__engine_v2: true */;
INSERT INTO test_ref_rel SELECT 1 /* __spqr__engine_v2: true */;

SELECT * FROM test_ref_rel ORDER BY i, j /*__spqr__execute_on: sh1 */;
SELECT * FROM test_ref_rel ORDER BY i, j /*__spqr__execute_on: sh2 */;
SELECT * FROM test_ref_rel ORDER BY i, j /*__spqr__execute_on: sh3 */;
SELECT * FROM test_ref_rel ORDER BY i, j /*__spqr__execute_on: sh4 */;

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

/* Check exeсution for returning clause */

TRUNCATE test_ref_rel;


INSERT INTO test_ref_rel VALUES(1,3),(2,4),(3,-1) RETURNING *;


-- check data on partially distributed reference relation
SELECT * FROM test_ref_rel ORDER BY i, j /*__spqr__execute_on: sh1 */;
SELECT * FROM test_ref_rel ORDER BY i, j /*__spqr__execute_on: sh2 */;
SELECT * FROM test_ref_rel ORDER BY i, j /*__spqr__execute_on: sh3 */;
SELECT * FROM test_ref_rel ORDER BY i, j /*__spqr__execute_on: sh4 */;


DROP TABLE test_ref_rel;
DROP TABLE test_ref_rel_part;


\c spqr-console
SHOW reference_relations;
DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;

