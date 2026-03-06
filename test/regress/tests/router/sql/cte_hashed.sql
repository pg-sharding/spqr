\c spqr-console

CREATE DISTRIBUTION ds1 (integer HASH);

CREATE KEY RANGE FROM 3221225472 ROUTE TO sh4 FOR DISTRIBUTION ds1;
CREATE KEY RANGE FROM 2147483648 ROUTE TO sh3 FOR DISTRIBUTION ds1;

CREATE KEY RANGE FROM 1073741824 ROUTE TO sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;

CREATE REFERENCE TABLE ref_rel_1;

CREATE DISTRIBUTED RELATION table1 (i HASH MURMUR) FOR DISTRIBUTION ds1;
CREATE DISTRIBUTED RELATION table2 (a HASH MURMUR) FOR DISTRIBUTION ds1;

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

select __spqr__ctid ('table1');

INSERT INTO table2 (a,b,c) VALUES (1, 22, 33), (2, 23, 34), (3, 24, 35), (4, 25, 36);

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

select __spqr__ctid ('table2');

with vals (z) as (values (1), (44), (55), (88)) insert into table2 (a) select z from vals;

select __spqr__ctid ('table2');

TRUNCATE table2;

with vals (z) as (values (1), (2), (288), (388), (188)) insert into table2 (a) select z from vals;

select __spqr__ctid ('table2');

TRUNCATE table2;

-- should create overwrite query map
with vals (i) as (values (101), (288)),
	z as (insert into table2 (a)
		select i from vals returning *) table z;

select __spqr__ctid ('table2');

TRUNCATE table2;

-- should create overwrite query map
with vals (i) as (values (101), (288)),
	z as (insert into table2 (a) select i from vals returning *),
		zz as (update table2 t set c = 12 from vals v where t.a = v.i returning t.*)
			table z union all table zz;

select __spqr__ctid ('table2');

INSERT INTO ref_rel_1 (i, j) VALUES (100, 100);

with vals (i) as (values(100)), vals_ext as (select vals.* from vals join ref_rel_1 rf on rf.i=vals.i) select * from table2 t join vals_ext v on t.a = v.i;
with vals (i) as (values(101)), vals_ext as (select vals.* from vals join ref_rel_1 rf on rf.i=vals.i) select * from table2 t join vals_ext v on t.a = v.i;

with vals( b,c, a,d) as (values( 1, 2, 233, 4)) insert into table2 (b,a) select b,a from vals returning *;

TRUNCATE table2;

with vals( b,c, a,d) as (values ( 1, 2, 233, 4), ( 1, 2, 133, 4), ( 1, 2, 132, 4), ( 1, 2, 33, 4)) insert into table2 (b,a) select b,a from vals returning *;

select __spqr__ctid ('table2');

DROP TABLE table1;
DROP TABLE table2;
DROP TABLE ref_rel_1;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;


