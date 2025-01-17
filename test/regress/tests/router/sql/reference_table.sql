\c spqr-console

-- test both ways of ref relation crete syntax
CREATE REFERENCE TABLE test_ref_rel;

\c regress

CREATE TABLE test_ref_rel(i int, j int);

COPY test_ref_rel FROM STDIN;
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

UPDATE test_ref_rel SET i = i + 1 /* __spqr__engine_v2: true */;

UPDATE test_ref_rel SET i = - i WHERE i IN (3, 4) /* __spqr__engine_v2: true */;

SELECT * FROM test_ref_rel ORDER BY i, j /*__spqr__execute_on: sh1 */;
SELECT * FROM test_ref_rel ORDER BY i, j /*__spqr__execute_on: sh2 */;

DELETE FROM test_ref_rel WHERE i = 2 /* __spqr__engine_v2: true */;

SELECT * FROM test_ref_rel ORDER BY i, j /*__spqr__execute_on: sh1 */;
SELECT * FROM test_ref_rel ORDER BY i, j /*__spqr__execute_on: sh2 */;

DROP TABLE test_ref_rel;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;

