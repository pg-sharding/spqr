\c spqr-console

CREATE DISTRIBUTION ds1 COLUMN TYPES integer;

CREATE KEY RANGE krid1 FROM 1 ROUTE TO sh1 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid2 FROM 11 ROUTE TO sh1 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid3 FROM 21 ROUTE TO sh2 FOR DISTRIBUTION ds1;

ALTER DISTRIBUTION ds1 ATTACH RELATION xx DISTRIBUTION KEY w_id;
ALTER DISTRIBUTION ds1 ATTACH RELATION xxerr DISTRIBUTION KEY id;
ALTER DISTRIBUTION ds1 ATTACH RELATION xxtt1 DISTRIBUTION KEY w_id;

\c regress
CREATE TABLE xx (w_id int);
CREATE TABLE xxerr (i int);
CREATE TABLE xxtt1 (i int, j int, w_id int);

INSERT INTO xx (w_id) VALUES (1);
INSERT INTO xx (w_id) VALUES (10);
INSERT INTO xx (w_id) VALUES (20);
INSERT INTO xx (w_id) VALUES (21);
INSERT INTO xx (w_id) VALUES (30);


SELECT * FROM xx WHERE w_id >= 1;
SELECT * FROM xx WHERE w_id >= 20;
SELECT * FROM xx WHERE w_id >= 21;

INSERT INTO xxtt1 (w_id) VALUES(1);
INSERT INTO xxtt1 (w_id) VALUES(15);
INSERT INTO xxtt1 (w_id) VALUES(21);

INSERT INTO xxtt1 (i, w_id) VALUES(1, 1);
INSERT INTO xxtt1 (i, w_id) VALUES(15, 15);
INSERT INTO xxtt1 (i, w_id) VALUES(21, 21);

INSERT INTO xxtt1 (w_id, i) VALUES(1, 1);
INSERT INTO xxtt1 (w_id, i) VALUES(15, -12);
INSERT INTO xxtt1 (w_id, i) VALUES(21, 12);

INSERT INTO xxtt1 (i, w_id) VALUES(1, 1);
INSERT INTO xxtt1 (i, w_id) VALUES(-12, 15);
INSERT INTO xxtt1 (i, w_id) VALUES(2121221, 21);

INSERT INTO xxtt1 (i, j, w_id) VALUES(-12, 1, 1);
INSERT INTO xxtt1 (i, w_id, j) VALUES(-12, 15, 123123);
INSERT INTO xxtt1 (j, i, w_id) VALUES(2121221, -211212, 23);
INSERT INTO xxtt1 (j, i, w_id) VALUES(2121221, -211212, 21);
INSERT INTO xxtt1 (j, i, w_id) VALUES(2121221, -211212, 21);
INSERT INTO xxtt1 (j, i, w_id) VALUES(2121221, -211212, 21);
INSERT INTO xxtt1 (j, i, w_id) VALUES(2121221, -211212, 21);

SELECT * FROM xxtt1 WHERE w_id >= 1;
SELECT * FROM xxtt1 WHERE w_id >= 20;
SELECT * FROM xxtt1 WHERE w_id >= 21;
SELECT DISTINCT * FROM xxtt1 WHERE w_id=21;

-- check that aliases works
SELECT * FROM xxtt1 a WHERE a.w_id >= 1;
SELECT * FROM xxtt1 a WHERE a.w_id >= 20;
SELECT * FROM xxtt1 a WHERE a.w_id >= 21;


SELECT * FROM xxtt1 a WHERE a.w_id = 21 and j + i != 0;
SELECT * FROM xxtt1 a WHERE a.w_id = 21 and w_id <= 30 and j + i != 0;

-- check that `INSERT FROM SELECT` works
INSERT INTO xx SELECT * FROM xx a WHERE a.w_id = 20;
SELECT * FROM xx WHERE w_id >= 20;

-- check that `INSERT FROM SELECT` with constant works
INSERT INTO xx (w_id) SELECT 20;
SELECT * FROM xx WHERE w_id >= 20;
INSERT INTO xxtt1 (j, w_id) SELECT a, 20 from unnest(ARRAY[10]) a;
SELECT * FROM xxtt1 WHERE w_id = 20;

-- check that complex UPDATE works
UPDATE xxtt1 set i=a.i, j=a.j from unnest(ARRAY[(1,10)]) as a(i int, j int) where w_id=20 and xxtt1.j=a.j;
SELECT * FROM xxtt1 WHERE w_id = 20;

DROP TABLE xx;
DROP TABLE xxtt1;
DROP TABLE xxerr;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;