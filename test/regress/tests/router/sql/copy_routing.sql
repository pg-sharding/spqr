\c spqr-console
CREATE DISTRIBUTION ds1 COLUMN TYPES int;
CREATE KEY RANGE FROM 300 ROUTE TO sh4 FOR DISTRIBUTION ds1;
CREATE KEY RANGE FROM 200 ROUTE TO sh3 FOR DISTRIBUTION ds1;
CREATE KEY RANGE FROM 100 ROUTE TO sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE FROM 1 ROUTE TO sh1 FOR DISTRIBUTION ds1;
ALTER DISTRIBUTION ds1 ATTACH RELATION copy_test DISTRIBUTION KEY id;
ALTER DISTRIBUTION ds1 ATTACH RELATION copy_test_mult DISTRIBUTION KEY id;

\c regress
CREATE TABLE copy_test (id int);
CREATE TABLE copy_test_mult (id int, uid int);

COPY copy_test(id) FROM STDIN;
1
2
3
5
10
15
200
199
99
290
120
312
300
310
301
299
399
199
99
201
301
101
10001
7
\.

SELECT * FROM copy_test ORDER BY id /* __spqr__execute_on: sh1 */;
SELECT * FROM copy_test ORDER BY id /* __spqr__execute_on: sh2 */;
SELECT * FROM copy_test ORDER BY id /* __spqr__execute_on: sh3 */;
SELECT * FROM copy_test ORDER BY id /* __spqr__execute_on: sh4 */;

COPY copy_test(id) FROM STDIN;
16
116
216
316
616
\.

SELECT * FROM copy_test ORDER BY id /* __spqr__execute_on: sh1 */;
SELECT * FROM copy_test ORDER BY id /* __spqr__execute_on: sh2 */;
SELECT * FROM copy_test ORDER BY id /* __spqr__execute_on: sh3 */;
SELECT * FROM copy_test ORDER BY id /* __spqr__execute_on: sh4 */;

TRUNCATE copy_test;

/* test conditional copy */

COPY copy_test(id) FROM STDIN WHERE id <= 10 OR id % 2 = 1;
1
2
3
4
5
6
10
11
12
14
100
101
220
222
200
201
300
301
299
199
99
98
198
202
\.

SELECT * FROM copy_test ORDER BY id /* __spqr__execute_on: sh1 */;
SELECT * FROM copy_test ORDER BY id /* __spqr__execute_on: sh2 */;
SELECT * FROM copy_test ORDER BY id /* __spqr__execute_on: sh3 */;
SELECT * FROM copy_test ORDER BY id /* __spqr__execute_on: sh4 */;


/* copy with several columns */

COPY copy_test_mult (id, uid) FROM stdin;
1	14
2	14
101	14
165	14
16	14
202	14
203	14
199	14
99	14
2012021	14
\.

SELECT * FROM copy_test_mult ORDER BY id /* __spqr__execute_on: sh1 */;
SELECT * FROM copy_test_mult ORDER BY id /* __spqr__execute_on: sh2 */;
SELECT * FROM copy_test_mult ORDER BY id /* __spqr__execute_on: sh3 */;
SELECT * FROM copy_test_mult ORDER BY id /* __spqr__execute_on: sh4 */;

COPY copy_test_mult (uid, id) FROM stdin;
7	16
7	116
7	216
7	316
\.

SELECT * FROM copy_test_mult ORDER BY id /* __spqr__execute_on: sh1 */;
SELECT * FROM copy_test_mult ORDER BY id /* __spqr__execute_on: sh2 */;
SELECT * FROM copy_test_mult ORDER BY id /* __spqr__execute_on: sh3 */;
SELECT * FROM copy_test_mult ORDER BY id /* __spqr__execute_on: sh4 */;

COPY copy_test_mult (uid, id) FROM stdin;
7	16
7
\.

DROP TABLE copy_test;
DROP TABLE copy_test_mult;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;
