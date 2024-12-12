\c spqr-console

CREATE REPLICATED DISTRIBUTION;
ALTER REPLICATED DISTRIBUTION ATTACH RELATION test_ref_rel;

-- test both ways of ref relation crete syntax
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

TABLE test_ref_rel;
TABLE test_ref_rel_2;

DROP TABLE test_ref_rel;
DROP TABLE test_ref_rel_2;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;

