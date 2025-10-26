\c spqr-console

CREATE DISTRIBUTION ds1 COLUMN TYPES integer;

CREATE KEY RANGE FROM 301 ROUTE TO sh4 FOR DISTRIBUTION ds1;
CREATE KEY RANGE FROM 201 ROUTE TO sh3 FOR DISTRIBUTION ds1;

CREATE KEY RANGE FROM 101 ROUTE TO sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE FROM 1 ROUTE TO sh1 FOR DISTRIBUTION ds1;

CREATE REFERENCE TABLE ref_rel_1;

ALTER DISTRIBUTION ds1 ATTACH RELATION table1 DISTRIBUTION KEY i;

\c regress

CREATE TABLE table1(i INT PRIMARY KEY);
CREATE TABLE ref_rel_1(i int, j int);

-- with engine v2 this should NOT be dispatched
SELECT i, (SELECT count(*) from table1) FROM table1 WHERE i = 101;

-- TODO: test this
--SELECT i, (SELECT count(*) from ref_rel_1) FROM ref_rel_1;


-- with engine v2 this should NOT be dispatched
SELECT i, (SELECT count(*) from ref_rel_1) FROM table1 t WHERE t.i = 201;

--SELECT i, (SELECT count(*) from table1) FROM ref_rel_1;

DROP TABLE table1;
DROP TABLE ref_rel_1;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;

