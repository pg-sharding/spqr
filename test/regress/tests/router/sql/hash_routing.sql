\c spqr-console

-- SETUP
CREATE DISTRIBUTION ds1 COLUMN TYPES varchar hash;

CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid2 FROM 2147483648 ROUTE TO sh2 FOR DISTRIBUTION ds1;

-- the set of all unsigned 32-bit integers (0 to 4294967295)
ALTER DISTRIBUTION ds1 ATTACH RELATION xx DISTRIBUTION KEY col1 HASH FUNCTION MURMUR;

-- TEST
\c regress
CREATE TABLE xx (col1 varchar);
INSERT INTO xx (col1) VALUES ('Hello, world!');
INSERT INTO xx (col1) VALUES ('test');
INSERT INTO xx (col1) VALUES ('众口难调');
INSERT INTO xx (col1) VALUES ('The quick brown fox jumps over the lazy dog');
INSERT INTO xx (col1) VALUES ('Армия — не просто доброе слово, а очень быстрое дело. Так мы выигрывали все войны. Пока противник рисует карты наступления, мы меняем ландшафты, причём вручную. Когда приходит время атаки, противник теряется на незнакомой местности и приходит в полную небоеготовность. В этом смысл, в этом наша стратегия.');

--TEARDOWN
DROP TABLE xx;
\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;