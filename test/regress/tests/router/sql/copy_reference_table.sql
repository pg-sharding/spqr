\c spqr-console

CREATE REPLICATED DISTRIBUTION;
ALTER REPLICATED DISTRIBUTION ATTACH RELATION test_ref_rel;

\c regress

CREATE TABLE test_ref_rel(i int, j int);


COPY test_ref_rel FROM STDIN;
1	2
2	3
3	4
4	5
\.


TABLE test_ref_rel;

DROP TABLE test_ref_rel;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;

