
\c spqr-console
CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
CREATE KEY RANGE kridi1 from 0 route to sh1 FOR DISTRIBUTION ds1;
CREATE KEY RANGE kridi2 from 11 route to sh2 FOR DISTRIBUTION ds1;
ALTER DISTRIBUTION ds1 ATTACH RELATION xjoin DISTRIBUTION KEY id;
ALTER DISTRIBUTION ds1 ATTACH RELATION yjoin DISTRIBUTION KEY w_id;

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

SELECT * FROM xjoin JOIN yjoin on id=w_id where yjoin.w_id = 15 ORDER BY id;
SELECT * FROM xjoin JOIN yjoin on id=w_id where w_id = 15 ORDER BY id;

DROP TABLE xjoin;
DROP TABLE yjoin;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;