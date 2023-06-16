--CREATE DATABASE regressiondb IF NOT EXISTS;
\c spqr-console
DROP KEY RANGE ALL;
DROP SHARDING RULE ALL;
CREATE SHARDING RULE r1 COLUMN i;
CREATE KEY RANGE kridi1 from 0 to 10 route to sh1;
CREATE KEY RANGE kridi2 from 11 to 20 route to sh2;

\c regressiondb

CREATE TABLE sshjt1(i int, j int);

INSERT INTO sshjt1 (i, j) VALUES(1, 12);

INSERT INTO sshjt1 (i, j) VALUES(12, 12);
INSERT INTO sshjt1 (i, j) VALUES(12, 13);

SELECT * FROM sshjt1 WHERE i = 12;
SELECT * FROM sshjt1 WHERE i = 12 AND  j =1;

SELECT * FROM sshjt1 a join sshjt1 b WHERE a.i = 12 ON TRUE;
SELECT * FROM sshjt1 a join sshjt1 b ON TRUE WHERE a.i = 12;

SELECT * FROM sshjt1 a join sshjt1 b ON TRUE WHERE a.i = 12 AND b.j = a.j;

DROP TABLE sshjt1;
\c db1
