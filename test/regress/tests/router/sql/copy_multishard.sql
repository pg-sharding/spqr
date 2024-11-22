\c spqr-console
-- SETUP
CREATE DISTRIBUTION ds1 COLUMN TYPES int hash;

CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid2 FROM 2147483648 ROUTE TO sh2 FOR DISTRIBUTION ds1;

-- the set of all unsigned 32-bit integers (0 to 4294967295)
ALTER DISTRIBUTION ds1 ATTACH RELATION xx DISTRIBUTION KEY i HASH FUNCTION MURMUR;

-- TEST
\c regress
CREATE TABLE xx (i int, j int);

COPY xx (i, j) FROM STDIN WITH DELIMITER '|' /* __spqr__allow_multishard: true */;
1|1
2|2
3|3
4|4
5|5
6|6
7|7
8|8
9|9
10|10
\.

INSERT INTO xx (i, j) VALUES(1,1);
INSERT INTO xx (i, j) VALUES(2,2);
INSERT INTO xx (i, j) VALUES(3,3);
INSERT INTO xx (i, j) VALUES(4,4);
INSERT INTO xx (i, j) VALUES(5,5);
INSERT INTO xx (i, j) VALUES(6,6);
INSERT INTO xx (i, j) VALUES(7,7);
INSERT INTO xx (i, j) VALUES(8,8);
INSERT INTO xx (i, j) VALUES(9,9);
INSERT INTO xx (i, j) VALUES(10,10);

TABLE xx /* __spqr__execute_on: sh1 */;
TABLE xx /* __spqr__execute_on: sh2 */;

DROP TABLE xx;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;


