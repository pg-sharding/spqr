\c spqr-console
CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
CREATE KEY RANGE kridi1 from 0 route to sh1 FOR DISTRIBUTION ds1;
CREATE KEY RANGE kridi2 from 11 route to sh2 FOR DISTRIBUTION ds1;
ALTER DISTRIBUTION ds1 ATTACH RELATION sshjt1 DISTRIBUTION KEY i;

\c regress

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

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;