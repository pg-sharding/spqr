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

-- UUID HASH
\c spqr-console
CREATE DISTRIBUTION ds_uuid COLUMN TYPES uuid hash;

CREATE KEY RANGE uuid_kr2 FROM 2147483648 ROUTE TO sh2 FOR DISTRIBUTION ds_uuid;
CREATE KEY RANGE uuid_kr1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds_uuid;

ALTER DISTRIBUTION ds_uuid ATTACH RELATION xxhashuuid DISTRIBUTION KEY id HASH FUNCTION MURMUR;

\c regress
CREATE TABLE xxhashuuid (id uuid);
INSERT INTO xxhashuuid (id) VALUES ('018f4b8e-37f0-7cc4-b5f2-0f62d09ca662');
INSERT INTO xxhashuuid (id) VALUES ('018F4B8E-37F0-7CC4-B5F2-0F62D09CA663');
INSERT INTO xxhashuuid (id) VALUES ('00000000-0000-0000-0000-000000000000');
INSERT INTO xxhashuuid (id) VALUES ('ffffffff-ffff-ffff-ffff-ffffffffffff');

SELECT * FROM xxhashuuid WHERE id = '018f4b8e-37f0-7cc4-b5f2-0f62d09ca662';
SELECT * FROM xxhashuuid WHERE id = '018F4B8E-37F0-7CC4-B5F2-0F62D09CA663';
SELECT __spqr__ctid('xxhashuuid');

SELECT * FROM xxhashuuid ORDER BY id /* __spqr__execute_on: sh1 */;
SELECT * FROM xxhashuuid ORDER BY id /* __spqr__execute_on: sh2 */;

COPY xxhashuuid (id) FROM STDIN;
018f4b8e-37f0-7cc4-b5f2-0f62d09ca662
018F4B8E-37F0-7CC4-B5F2-0F62D09CA663
00000000-0000-0000-0000-000000000000
ffffffff-ffff-ffff-ffff-ffffffffffff
\.

SELECT * FROM xxhashuuid ORDER BY id /* __spqr__execute_on: sh1 */;
SELECT * FROM xxhashuuid ORDER BY id /* __spqr__execute_on: sh2 */;

--TEARDOWN
DROP TABLE xxhash;
DROP TABLE xxhashuuid;
\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
