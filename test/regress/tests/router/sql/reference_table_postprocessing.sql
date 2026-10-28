\c spqr-console

CREATE REFERENCE TABLE test_ref_rel;

\c regress

CREATE TABLE test_ref_rel(i int, j int);

COPY test_ref_rel FROM STDIN;
1	2
2	3
3	4
4	5
\.

set __spqr__allow_postprocessing to on;

-- used to fail with "in-router postprocessing for GROUP BY aggregate is not yet supported"
SELECT i, COUNT(*) FROM test_ref_rel WHERE i IN (1, 2) GROUP BY i ORDER BY i /* __spqr__execute_on: sh1 */;
SELECT i, SUM(j) FROM test_ref_rel GROUP BY i ORDER BY i /* __spqr__execute_on: sh1 */;
SELECT i, MIN(j), MAX(j) FROM test_ref_rel GROUP BY i ORDER BY i /* __spqr__execute_on: sh2 */;
SELECT COUNT(j) FROM test_ref_rel /* __spqr__execute_on: sh1 */;
SELECT i FROM test_ref_rel ORDER BY i DESC LIMIT 2 /* __spqr__execute_on: sh1 */;
SELECT i, COUNT(*) FROM test_ref_rel WHERE i > 100 GROUP BY i /* __spqr__execute_on: sh1 */;

DROP TABLE test_ref_rel;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
