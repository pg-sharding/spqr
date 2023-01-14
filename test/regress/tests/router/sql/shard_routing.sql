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

SELECT * FROM xxtt1 WHERE w_id >= 1;
SELECT * FROM xxtt1 WHERE w_id >= 20;
SELECT * FROM xxtt1 WHERE w_id >= 21;

-- check that aliases works
SELECT * FROM xxtt1 a WHERE a.w_id >= 1;
SELECT * FROM xxtt1 a WHERE a.w_id >= 20;
SELECT * FROM xxtt1 a WHERE a.w_id >= 21;

DROP TABLE xx;
DROP TABLE xxtt1;

