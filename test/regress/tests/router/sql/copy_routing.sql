\c spqr-console
ADD SHARDING RULE t1 COLUMNS id;
ADD KEY RANGE krid1 FROM 1 TO 30 ROUTE TO sh1;
ADD KEY RANGE krid2 FROM 30 TO 4001 ROUTE TO sh2;

\c regress
CREATE TABLE copy_test (id int);

COPY copy_test FROM STDIN WHERE id <= 10;
1
2
3
4
5
12
3434
43
\.

SELECT * FROM copy_test WHERE id <= 10;

COPY copy_test FROM STDIN WHERE id <= 30;
1
2
3
4
5
12
23
22
32
42
3434
43
\.

SELECT * FROM copy_test WHERE id <= 30 ORDER BY copy_test;

DROP TABLE copy_test;

\c spqr-console

DROP KEY RANGE ALL;
DROP SHARDING RULE ALL;
