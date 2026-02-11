\c spqr-console

CREATE DISTRIBUTION ds1 COLUMN TYPES integer;

CREATE KEY RANGE FROM 20 ROUTE TO sh4 FOR DISTRIBUTION ds1;
CREATE KEY RANGE FROM 10 ROUTE TO sh3 FOR DISTRIBUTION ds1;

CREATE KEY RANGE FROM -10 ROUTE TO sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE FROM -20 ROUTE TO sh1 FOR DISTRIBUTION ds1;


CREATE DISTRIBUTED RELATION nr_table1 DISTRIBUTION KEY i IN ds1;

\c regress

CREATE TABLE nr_table1(i INT);

INSERT INTO nr_table1(i) values(100);
INSERT INTO nr_table1(i) values(21);
INSERT INTO nr_table1(i) values(19);
INSERT INTO nr_table1(i) values(10);
INSERT INTO nr_table1(i) values(9); 
INSERT INTO nr_table1(i) values(0); 
INSERT INTO nr_table1(i) values(-1);
INSERT INTO nr_table1(i) values(-9);
INSERT INTO nr_table1(i) values(-10);
INSERT INTO nr_table1(i) values(-11);
INSERT INTO nr_table1(i) values(-19);
INSERT INTO nr_table1(i) values(-21);
INSERT INTO nr_table1(i) values(-100);


SELECT * FROM nr_table1 WHERE i = 100;
SELECT * FROM nr_table1 WHERE i = 21;
SELECT * FROM nr_table1 WHERE i = 19;
SELECT * FROM nr_table1 WHERE i = 10;
SELECT * FROM nr_table1 WHERE i = 9;
SELECT * FROM nr_table1 WHERE i = 0;
SELECT * FROM nr_table1 WHERE i = -1;
SELECT * FROM nr_table1 WHERE i = -9;
SELECT * FROM nr_table1 WHERE i = -10;
SELECT * FROM nr_table1 WHERE i = -11;
SELECT * FROM nr_table1 WHERE i = -19;
SELECT * FROM nr_table1 WHERE i = -21;
SELECT * FROM nr_table1 WHERE i = -100;

DROP TABLE nr_table1;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
