\c spqr-console
DROP KEY RANGE ALL;
DROP SHARDING RULE ALL;

ADD SHARDING RULE r1 COLUMNS w_id;
ADD SHARDING RULE r2 COLUMNS id;

ADD KEY RANGE krid1 FROM 1 TO 20 ROUTE TO sh1;
ADD KEY RANGE krid2 FROM 21 TO 30 ROUTE TO sh2;

\c regress
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

SELECT * FROM xjoin JOIN yjoin on id=w_id ORDER BY id;
