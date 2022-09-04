DROP TABLE IF EXISTS xxcopy;
CREATE TABLE xxcopy (id int);

COPY xxcopy FROM STDIN WHERE id <= 10;
1
2
3
4
5
12
3434
43
\.

SELECT * FROM xxcopy WHERE id <= 10;

COPY xxcopy FROM STDIN WHERE id <= 30;
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

SELECT * FROM xxcopy WHERE id <= 30 ORDER BY xxcopy;