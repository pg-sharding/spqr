\c spqr-console

CREATE DISTRIBUTION ds1 COLUMN TYPES integer;

CREATE KEY RANGE FROM 301 ROUTE TO sh4 FOR DISTRIBUTION ds1;
CREATE KEY RANGE FROM 201 ROUTE TO sh3 FOR DISTRIBUTION ds1;

CREATE KEY RANGE FROM 101 ROUTE TO sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE FROM 1 ROUTE TO sh1 FOR DISTRIBUTION ds1;

CREATE REFERENCE TABLE ref_rel_1;

ALTER DISTRIBUTION ds1 ATTACH RELATION table1 DISTRIBUTION KEY i;
CREATE DISTRIBUTED RELATION table2 DISTRIBUTION KEY a IN ds1;

\c regress

CREATE TABLE table1(i INT PRIMARY KEY);
CREATE TABLE table2(a INT, b INT, c INT);
CREATE TABLE ref_rel_1(i int, j int);

WITH s AS (
	SELECT 1 FROM table1 WHERE i = 10
) TABLE s;

WITH s AS (
	SELECT 1 FROM table1 WHERE i = 10
), s2 AS (SELECT * FROM s) TABLE s2;

WITH s AS (
	SELECT 1 FROM table1 WHERE i = 210
), s2 AS (SELECT * FROM table1 WHERE i = 288) TABLE s2;


-- XXX: fix parser to allow this
--WITH s AS (
--	SELECT 1 FROM table1 WHERE i = 210
--), s2 AS (SELECT * FROM table1 WHERE i = 288) INSERT INTO table1 (select * from s2 union all select * from s);

INSERT INTO table1 (i) VALUES(10);
WITH vv AS (SELECT 1) INSERT INTO table1 (i) VALUES(11);

WITH vv AS (SELECT i + 1 FROM table1 WHERE i = 11) INSERT INTO table1 (i) TABLE vv;

INSERT INTO table1 WITH s AS (SELECT i + 1 FROM table1 WHERE i = 12) TABLE s;

SELECT * FROM table1 ORDER BY i /* __spqr__execute_on: sh1 */;
SELECT * FROM table1 ORDER BY i /* __spqr__execute_on: sh2 */;
SELECT * FROM table1 ORDER BY i /* __spqr__execute_on: sh3 */;
SELECT * FROM table1 ORDER BY i /* __spqr__execute_on: sh4 */;

WITH vv (x, y, z) AS (VALUES (1, 2, 3)) SELECT * FROM table2 t WHERE t.a = vv.x;

DROP TABLE table1;
DROP TABLE table2;
DROP TABLE ref_rel_1;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;
