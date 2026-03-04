\c spqr-console

CREATE DISTRIBUTION ds1 COLUMN TYPES integer;

CREATE KEY RANGE FROM 301 ROUTE TO sh4 FOR DISTRIBUTION ds1;
CREATE KEY RANGE FROM 201 ROUTE TO sh3 FOR DISTRIBUTION ds1;

CREATE KEY RANGE FROM 101 ROUTE TO sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;

CREATE REFERENCE TABLE ref_rel_1;

ALTER DISTRIBUTION ds1 ATTACH RELATION table1 DISTRIBUTION KEY i;
CREATE DISTRIBUTED RELATION table2 (a) FOR DISTRIBUTION ds1;

\c regress

CREATE TABLE table1(i INT PRIMARY KEY);
CREATE TABLE table2(a INT, b INT, c INT);
CREATE TABLE ref_rel_1(i int, j int);

WITH s AS (
	SELECT 1 FROM table1 WHERE i = 10
) TABLE s;

WITH s AS (
	SELECT 1 FROM table1 WHERE i = 10
) SELECT 1;

WITH s AS (
	SELECT 1 FROM table1 WHERE i = 10
) SELECT (select * from s), 2;

WITH s AS (
	SELECT 1 FROM table1 WHERE i = 10
) SELECT 1 + 2;

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

INSERT INTO table2 (a,b,c) VALUES (1, 22, 33);
INSERT INTO table2 (a,b,c) VALUES (2, 22, 33);
INSERT INTO table2 (a,b,c) VALUES (101, 22, 33);
INSERT INTO table2 (a,b,c) VALUES (301, 22, 33);

WITH vv (x, y, z) AS (VALUES (1, 2, 3)) SELECT * FROM table2 t, vv WHERE t.a = vv.x;
WITH vv (x, y, z) AS (VALUES (1, 2, 3)) SELECT * FROM table2 t, vv v WHERE t.a = v.x;


WITH vals (x, y, z) AS (
    VALUES (
		1,
		2,
		4
    )
)
SELECT 
	*
FROM table2 r
JOIN vals 
	ON r.a = vals.x;


WITH vals (x) AS (
    VALUES (
		1
    ), (2),
	(101),
	(301)
)
SELECT 
	*
FROM table2 r
JOIN vals 
	ON r.a = vals.x;


WITH vals (y, z, x) AS (
    VALUES (
		2,
		4,
		1
    )
)
SELECT 
	*
FROM table2 r
JOIN vals 
	ON r.a = vals.x;

WITH vals (x, y, z) AS (
    VALUES (
		1,
		2,
		4
    )
),
ttttt AS (
    SELECT 
		*
    FROM table2 r
    JOIN vals 
        ON r.a = vals.x
)
UPDATE table2 SET b = b + 1;

SELECT * FROM table2 ORDER BY a /* __spqr__execute_on: sh1 */;

with vals (z) as (values (1), (88)) insert into table2 (a) select z from vals;

SELECT * FROM table2 ORDER BY a /* __spqr__execute_on: sh1 */;

TRUNCATE table2;

with vals (z) as (values (1), (188)) insert into table2 (a) select z from vals;

select __spqr__ctid ('table2');

TRUNCATE table2;

with vals (i) as (values (201), (288)),
	z as (insert into table2 (a)
		select i from vals returning *) table z;

-- should create overwrite query map
with vals (i) as (values (101), (288)),
	z as (insert into table2 (a)
		select i from vals returning *) table z;

select __spqr__ctid ('table2');

TRUNCATE table2;

with vals (i) as (values (201), (288)),
	z as (insert into table2 (a) select i from vals returning *),
		zz as (update table2 t set c = 12 from vals v where t.a = v.i returning t.*)
			table z union all table zz;

-- should create overwrite query map
with vals (i) as (values (101), (288)),
	z as (insert into table2 (a) select i from vals returning *),
		zz as (update table2 t set c = 12 from vals v where t.a = v.i returning t.*)
			table z union all table zz;

select __spqr__ctid ('table2');

INSERT INTO ref_rel_1 (i, j) VALUES (100, 100);

with vals (i) as (values(100)), vals_ext as (select vals.* from vals join ref_rel_1 rf on rf.i=vals.i) select * from table2 t join vals_ext v on t.a = v.i;
with vals (i) as (values(101)), vals_ext as (select vals.* from vals join ref_rel_1 rf on rf.i=vals.i) select * from table2 t join vals_ext v on t.a = v.i;

DROP TABLE table1;
DROP TABLE table2;
DROP TABLE ref_rel_1;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;


