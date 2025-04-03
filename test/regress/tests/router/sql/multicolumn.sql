\c spqr-console

-- check that numeric type works
CREATE DISTRIBUTION ds1 COLUMN TYPES integer, integer;
CREATE KEY RANGE FROM 0,0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
CREATE KEY RANGE FROM 0,100 ROUTE TO sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE FROM 100,0 ROUTE TO sh3 FOR DISTRIBUTION ds1;
CREATE KEY RANGE FROM 100,100 ROUTE TO sh4 FOR DISTRIBUTION ds1;

ALTER DISTRIBUTION ds1 ATTACH RELATION mcol_sh DISTRIBUTION KEY id, seq;

\c regress

CREATE TABLE mcol_sh(id INT, seq INT, val INT);

INSERT INTO mcol_sh (id, seq, val) VALUES (0, 10, 1);
INSERT INTO mcol_sh (id, seq, val) VALUES (0, 200, 1);

INSERT INTO mcol_sh (id, seq, val) VALUES (1, 1, 1);
INSERT INTO mcol_sh (id, seq, val) VALUES (2, 2, 1);

INSERT INTO mcol_sh (id, seq, val) VALUES (100, 10, 1);
INSERT INTO mcol_sh (id, seq, val) VALUES (100, 90, 1);

INSERT INTO mcol_sh (id, seq, val) VALUES (2000, 10, 1);
INSERT INTO mcol_sh (id, seq, val) VALUES (2000, 200, 1);

SELECT * FROM mcol_sh WHERE id = 0 AND seq = 10;
SELECT * FROM mcol_sh WHERE id = 0 AND seq = 200;
SELECT * FROM mcol_sh WHERE id = 1 AND seq = 1;
SELECT * FROM mcol_sh WHERE id = 2 AND seq = 2;
SELECT * FROM mcol_sh WHERE id = 100 AND seq = 10;
SELECT * FROM mcol_sh WHERE id = 100 AND seq = 90;
SELECT * FROM mcol_sh WHERE id = 2000 AND seq = 10;
SELECT * FROM mcol_sh WHERE id = 2000 AND seq = 200;

UPDATE mcol_sh SET val = val + 1 WHERE id = 2000 AND seq = 200;
SELECT * FROM mcol_sh WHERE id = 2000 AND seq = 200;

SELECT * FROM mcol_sh WHERE id = 2000 AND seq = 200 OR id = 2000 AND seq =10;

DROP TABLE mcol_sh;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;
