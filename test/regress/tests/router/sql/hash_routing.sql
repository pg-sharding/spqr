\c spqr-console

-- SETUP
CREATE DISTRIBUTION ds1 COLUMN TYPES varchar hash;

CREATE KEY RANGE krid2 FROM 2147483648 ROUTE TO sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;

-- the set of all unsigned 32-bit integers (0 to 4294967295)
ALTER DISTRIBUTION ds1 ATTACH RELATION xxhash DISTRIBUTION KEY col1 HASH FUNCTION MURMUR;

-- TEST
\c regress
CREATE TABLE xxhash (col1 varchar);
INSERT INTO xxhash (col1) VALUES ('Hello, world!');
INSERT INTO xxhash (col1) VALUES ('test');
INSERT INTO xxhash (col1) VALUES ('0');
INSERT INTO xxhash (col1) VALUES ('The quick brown fox jumps over the lazy dog');
INSERT INTO xxhash (col1) VALUES ('Армия — не просто доброе слово, а очень быстрое дело. Так мы выигрывали все войны. Пока противник рисует карты наступления, мы меняем ландшафты, причём вручную. Когда приходит время атаки, противник теряется на незнакомой местности и приходит в полную небоеготовность. В этом смысл, в этом наша стратегия.');

SELECT * FROM xxhash ORDER BY col1 /* __spqr__execute_on: sh1 */;
SELECT * FROM xxhash ORDER BY col1 /* __spqr__execute_on: sh2 */;

COPY xxhash (col1) FROM STDIN;
Hello, world!
test
0
The quick brown fox jumps over the lazy dog
Армия — не просто доброе слово, а очень быстрое дело. Так мы выигрывали все войны. Пока противник рисует карты наступления, мы меняем ландшафты, причём вручную. Когда приходит время атаки, противник теряется на незнакомой местности и приходит в полную небоеготовность. В этом смысл, в этом наша стратегия.
\.

SELECT * FROM xxhash ORDER BY col1 /* __spqr__execute_on: sh1 */;
SELECT * FROM xxhash ORDER BY col1 /* __spqr__execute_on: sh2 */;

--TEARDOWN
DROP TABLE xxhash;
\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
