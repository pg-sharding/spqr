DROP TABLE IF EXISTS xjoin;
CREATE TABLE xjoin(id int);

DROP TABLE IF EXISTS yjoin;
CREATE TABLE yjoin(w_id int);

INSERT INTO xjoin (id) values(1);
INSERT INTO xjoin (id) values(15);
INSERT INTO xjoin (id) values(25);

INSERT INTO yjoin (w_id) values(1);
INSERT INTO yjoin (w_id) values(15);
INSERT INTO yjoin (w_id) values(25);

SELECT * FROM xjoin JOIN yjoin on id=w_id;
