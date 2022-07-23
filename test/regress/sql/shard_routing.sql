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

DROP TABLE xx;
DROP TABLE xxtt1;

