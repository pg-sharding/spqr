\c spqr-console

CREATE REFERENCE TABLE test_ref_rel;
CREATE REFERENCE TABLE test_ref_rel_2;

\c regress

CREATE TABLE test_ref_rel(i int, j int);
CREATE TABLE test_ref_rel_2(i int, j int);


COPY test_ref_rel FROM STDIN;
1	2
2	3
3	4
4	5
\.

COPY test_ref_rel_2 FROM STDIN;
1	2
2	3
3	4
4	5
\.

TABLE test_ref_rel /* __spqr__execute_on: sh1 */;
TABLE test_ref_rel /* __spqr__execute_on: sh2 */;

TABLE test_ref_rel_2 /* __spqr__execute_on: sh1 */;
TABLE test_ref_rel_2 /* __spqr__execute_on: sh2 */;

DROP TABLE test_ref_rel;
DROP TABLE test_ref_rel_2;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;

