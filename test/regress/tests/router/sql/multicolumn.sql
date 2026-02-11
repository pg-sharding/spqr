\c spqr-console

-- check that numeric type works
CREATE DISTRIBUTION ds1 COLUMN TYPES integer, integer;
CREATE KEY RANGE FROM 100,100 ROUTE TO sh4 FOR DISTRIBUTION ds1;
CREATE KEY RANGE FROM 100,0 ROUTE TO sh3 FOR DISTRIBUTION ds1;
CREATE KEY RANGE FROM 0,100 ROUTE TO sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE FROM 0,0 ROUTE TO sh1 FOR DISTRIBUTION ds1;

ALTER DISTRIBUTION ds1 ATTACH RELATION mcol_sh DISTRIBUTION KEY id, seq;

-- check that different type works
CREATE DISTRIBUTION ds2 COLUMN TYPES integer, varchar;
CREATE KEY RANGE FROM 100, 'zzzzz' ROUTE TO sh4 FOR DISTRIBUTION ds2;
CREATE KEY RANGE FROM 100,    'aaaaa' ROUTE TO sh3 FOR DISTRIBUTION ds2;
CREATE KEY RANGE FROM 0,'zzzzz' ROUTE TO sh2 FOR DISTRIBUTION ds2;
CREATE KEY RANGE FROM 0,'aaaaa' ROUTE TO sh1 FOR DISTRIBUTION ds2;

ALTER DISTRIBUTION ds2 ATTACH RELATION mcol_dt DISTRIBUTION KEY id, addr;

\c regress

CREATE TABLE mcol_sh(id INT, seq INT, val INT);
CREATE TABLE mcol_dt(id INT, addr TEXT, val INT);

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

TRUNCATE mcol_sh;
COPY mcol_sh (id, seq, val) FROM stdin;
0	10	1
0	99	3
0	50	3
0	200	1
0	100	6
1	1	1
2	2	1
100	99	3
99	100	3
0	99	3
99	0	3
100	100	3
100	10	1
100	90	1
2000	10	1
2000	200	1
\.

SELECT * FROM mcol_sh ORDER BY 1,2,3 /*__spqr__execute_on: sh1*/;
SELECT * FROM mcol_sh ORDER BY 1,2,3 /*__spqr__execute_on: sh2*/;
SELECT * FROM mcol_sh ORDER BY 1,2,3 /*__spqr__execute_on: sh3*/;
SELECT * FROM mcol_sh ORDER BY 1,2,3 /*__spqr__execute_on: sh4*/;

TRUNCATE mcol_sh;
COPY mcol_sh (val, seq, id) FROM stdin;
1	10	0
3	99	0
3	50	0
1	200	0
6	100	0
1	1	1
1	2	1
3	99	100
3	100	99
3	99	0
3	0	99
3	100	100
1	10	100
1	90	100
1	10	2000
1	200	2000
\.

SELECT * FROM mcol_sh ORDER BY 1,2,3 /*__spqr__execute_on: sh1*/;
SELECT * FROM mcol_sh ORDER BY 1,2,3 /*__spqr__execute_on: sh2*/;
SELECT * FROM mcol_sh ORDER BY 1,2,3 /*__spqr__execute_on: sh3*/;
SELECT * FROM mcol_sh ORDER BY 1,2,3 /*__spqr__execute_on: sh4*/;

COPY mcol_dt (id, addr, val) FROM stdin;
0	aaaaa	1
0	zzzzz	2
50	sb	3
50	dssd	4
100	aaaa	5
100	aaaaa	6
150	zzzzz	7
150	zzzzz	8
100	zzzzz	9
\.

SELECT * FROM mcol_dt ORDER BY 1,2,3 /*__spqr__execute_on: sh1*/;
SELECT * FROM mcol_dt ORDER BY 1,2,3 /*__spqr__execute_on: sh2*/;
SELECT * FROM mcol_dt ORDER BY 1,2,3 /*__spqr__execute_on: sh3*/;
SELECT * FROM mcol_dt ORDER BY 1,2,3 /*__spqr__execute_on: sh4*/;


DROP TABLE mcol_sh;
DROP TABLE mcol_dt;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
