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
TABLE test_ref_rel;

set __spqr__default_route_behaviour to block;
TABLE test_ref_rel;

INSERT INTO test_ref_rel VALUES(1);
INSERT INTO test_ref_rel SELECT 1;

DROP TABLE test_ref_rel;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;

