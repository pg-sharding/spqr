DROP TABLE IF EXISTS xxmixed;
CREATE TABLE xxmixed(id int);
INSERT INTO xxmixed (id) VALUES(1);
INSERT INTO xxmixed (id) VALUES(10);
INSERT INTO xxmixed (id) VALUES(11);
INSERT INTO xxmixed (id) VALUES(20);

INSERT INTO xxmixed (id) VALUES(21);
INSERT INTO xxmixed (id) VALUES(22);
INSERT INTO xxmixed (id) VALUES(29);
INSERT INTO xxmixed (id) VALUES(30);

SELECT * FROM xxmixed;
SELECT * FROM xxmixed WHERE id <= 10;
SELECT * FROM xxmixed WHERE id <= 20;
SELECT * FROM xxmixed WHERE id <= 30;
