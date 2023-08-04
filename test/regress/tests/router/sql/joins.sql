
\c spqr-console
CREATE SHARDING RULE r1 COLUMN id;
CREATE SHARDING RULE r2 COLUMN w_id;
CREATE KEY RANGE kridi1 from 0 to 11 route to sh1;
CREATE KEY RANGE kridi2 from 11 to 31 route to sh2;

\c regress

CREATE TABLE xjoin(id int);
CREATE TABLE yjoin(w_id int);

INSERT INTO xjoin (id) values(1);
INSERT INTO xjoin (id) values(10);
INSERT INTO xjoin (id) values(15);
INSERT INTO xjoin (id) values(25);

INSERT INTO yjoin (w_id) values(1);
INSERT INTO yjoin (w_id) values(10);
INSERT INTO yjoin (w_id) values(15);
INSERT INTO yjoin (w_id) values(25);

SELECT * FROM xjoin JOIN yjoin on id=w_id ORDER BY id;
-- result is not full
--SELECT * FROM xjoin JOIN yjoin on true ORDER BY id;

SELECT * FROM xjoin JOIN yjoin on id=w_id where w_id = 15 ORDER BY id;

DROP TABLE xjoin;
DROP TABLE yjoin;

\c spqr-console
DROP KEY RANGE ALL;
DROP SHARDING RULE ALL;
