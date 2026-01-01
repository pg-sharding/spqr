\c spqr-console

-- check that numeric type works
CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
CREATE KEY RANGE krid4 FROM 300 ROUTE TO sh4 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid3 FROM 200 ROUTE TO sh3 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid2 FROM 100 ROUTE TO sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;

CREATE DISTRIBUTED RELATION xxmultish (id);

\c regress

SET __spqr__engine_v2 TO true;

CREATE TABLE xxmultish(id int, val int);

COPY xxmultish (id) FROM STDIN;
0
1
10
20
30
50
99
100
101
102
150
152
199
201
201
250
299
300
301
350
400
399
401
\.

/* XXX: sort result here is not stable until proper router processing support */

SELECT id FROM xxmultish ORDER BY id;

SELECT * FROM xxmultish WHERE id = 0 OR id = 199;
SELECT * FROM xxmultish WHERE id = 0 OR id = 399;
SELECT * FROM xxmultish WHERE id = 1 OR id = 299 OR id = 350;
SELECT * FROM xxmultish WHERE id = 299 OR id = 350;

SELECT * FROM xxmultish WHERE id = 201 UNION ALL SELECT * FROM xxmultish WHERE id = 199;
SELECT * FROM xxmultish WHERE id = 401 UNION ALL SELECT * FROM xxmultish WHERE id = 99;
SELECT * FROM xxmultish WHERE id = 401 EXCEPT ALL SELECT * FROM xxmultish WHERE id = 99;

WITH d AS (SELECT * FROM xxmultish WHERE id = 401 OR id = 0) TABLE d;

-- XXX: support this
--WITH d AS (SELECT * FROM xxmultish WHERE id = 401 OR id = 0) SELECT * FROM d UNION ALL SELECT * FROM xxmultish WHERE id = 300;

UPDATE xxmultish SET val = -1 /* __spqr__engine_v2: true */;
DELETE FROM xxmultish /* __spqr__engine_v2: true */;

DROP TABLE xxmultish;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;
