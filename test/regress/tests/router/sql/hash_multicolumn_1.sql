\c spqr-console

-- low-cardinality for first column is expected
CREATE DISTRIBUTION ds1 COLUMN TYPES INT, VARCHAR hash;

CREATE KEY RANGE krid4 FROM 1, 2147483648 ROUTE TO sh4 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid3 FROM 1, 0 ROUTE TO sh3 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid2 FROM 0, 2147483648 ROUTE TO sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid1 FROM 0, 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;

CREATE DISTRIBUTED RELATION hash_multi_xx DISTRIBUTION KEY col1, col2 HASH FUNCTION MURMUR IN ds1;

\c regress

CREATE TABLE hash_multi_xx(col1 INT, col2 TEXT);

INSERT INTO hash_multi_xx (col1, col2) VALUES(1, 'abwqqwqabba');
INSERT INTO hash_multi_xx (col1, col2) VALUES(0, 'abwqqwqabba');
INSERT INTO hash_multi_xx (col1, col2) VALUES(0, 'ababba');
INSERT INTO hash_multi_xx (col1, col2) VALUES(0, 'ababdfba');
INSERT INTO hash_multi_xx (col1, col2) VALUES(1, 'abawqqqwbba');
INSERT INTO hash_multi_xx (col1, col2) VALUES(1, 'ababxaxasba');
INSERT INTO hash_multi_xx (col1, col2) VALUES(1, 'ababxxxxba');
INSERT INTO hash_multi_xx (col1, col2) VALUES(1, 'ababbxxxa');
INSERT INTO hash_multi_xx (col1, col2) VALUES(0, 'ababbxxxa');
INSERT INTO hash_multi_xx (col1, col2) VALUES(1, 'dejwio');
INSERT INTO hash_multi_xx (col1, col2) VALUES(1, 'dejwiewdewo');
INSERT INTO hash_multi_xx (col1, col2) VALUES(1, '232892');
INSERT INTO hash_multi_xx (col1, col2) VALUES(0, '232892');

SELECT * FROM hash_multi_xx ORDER BY col1 /* __spqr__execute_on: sh1 */;
SELECT * FROM hash_multi_xx ORDER BY col1 /* __spqr__execute_on: sh2 */;
SELECT * FROM hash_multi_xx ORDER BY col1 /* __spqr__execute_on: sh3 */;
SELECT * FROM hash_multi_xx ORDER BY col1 /* __spqr__execute_on: sh4 */;

TRUNCATE hash_multi_xx;

COPY hash_multi_xx (col1, col2)  FROM STDIN DELIMITER '|';
1|abwqqwqabba
0|abwqqwqabba
0|ababba
0|ababdfba
1|abawqqqwbba
1|ababxaxasba
1|ababxxxxba
1|ababbxxxa
0|ababbxxxa
1|dejwio
1|dejwiewdewo
1|232892
0|232892
\.

SELECT * FROM hash_multi_xx ORDER BY col1 /* __spqr__execute_on: sh1 */;
SELECT * FROM hash_multi_xx ORDER BY col1 /* __spqr__execute_on: sh2 */;
SELECT * FROM hash_multi_xx ORDER BY col1 /* __spqr__execute_on: sh3 */;
SELECT * FROM hash_multi_xx ORDER BY col1 /* __spqr__execute_on: sh4 */;

DROP TABLE hash_multi_xx;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
