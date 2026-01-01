\c spqr-console

-- check that numeric type works
CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
CREATE KEY RANGE FROM 300 ROUTE TO sh4 FOR DISTRIBUTION ds1;
CREATE KEY RANGE FROM 200 ROUTE TO sh3 FOR DISTRIBUTION ds1;
CREATE KEY RANGE FROM 100 ROUTE TO sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;


CREATE DISTRIBUTED RELATION xxm_expd (id);

\c regress

SET __spqr__engine_v2 TO true;

CREATE TABLE xxm_expd(id int, j INT);

COPY xxm_expd (id, j) FROM STDIN;
1	1
2	2
3	3
4	4
5	5
50	50
51	51
100	100
101	101
199	199
200	200
201	201
299	299
300	300
301	301
399	399
\.


BEGIN;
UPDATE xxm_expd SET j = j + 1 WHERE id = 1 /* __spqr__engine_v2: true */;
UPDATE xxm_expd SET j = j + 1 WHERE id = 101 /* __spqr__engine_v2: true */;
UPDATE xxm_expd SET j = j + 1 WHERE id = 299 /* __spqr__engine_v2: true */;
UPDATE xxm_expd SET j = j + 1 WHERE id = 399 /* __spqr__engine_v2: true */;
COMMIT;

BEGIN;
INSERT INTO xxm_expd (id, j) VALUES(55, 55) /* __spqr__engine_v2: true */;
INSERT INTO xxm_expd (id, j) VALUES(155, 155) /* __spqr__engine_v2: true */;
INSERT INTO xxm_expd (id, j) VALUES(355, 355) /* __spqr__engine_v2: true */;
ROLLBACK;

BEGIN;
DELETE FROM xxm_expd /* __spqr__engine_v2: true */;
SELECT * FROM xxm_expd WHERE id = 12 /* __spqr__engine_v2: true */;
SELECT * FROM xxm_expd WHERE id = 212 /* __spqr__engine_v2: true */;
ROLLBACK;

BEGIN;
SELECT * FROM xxm_expd WHERE id = 12 /* __spqr__engine_v2: true */;
SELECT * FROM xxm_expd WHERE id = 212 /* __spqr__engine_v2: true */;
DELETE FROM xxm_expd /* __spqr__engine_v2: true */;
ROLLBACK;

SELECT * FROM xxm_expd ORDER BY id /* __spqr__engine_v2: true */;

UPDATE xxm_expd SET j = -1 /* __spqr__engine_v2: true */;
DELETE FROM xxm_expd /* __spqr__engine_v2: true */;

DROP TABLE xxm_expd;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;
