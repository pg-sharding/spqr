CREATE TABLE xx (w_id int);
CREATE TABLE xxtt1 (i int, j int, w_id int);

INSERT INTO xx (w_id) VALUES (1);
INSERT INTO xx (w_id) VALUES (10);
INSERT INTO xx (w_id) VALUES (20);
INSERT INTO xx (w_id) VALUES (21);
INSERT INTO xx (w_id) VALUES (30);

SELECT * FROM xx;

DROP TABLE xx;
DROP TABLE xxtt1;

