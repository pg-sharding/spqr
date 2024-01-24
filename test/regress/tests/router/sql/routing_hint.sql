\c spqr-console

CREATE SHARDING RULE t1 COLUMNS id;
CREATE KEY RANGE krid1 FROM 1 ROUTE TO sh1;
CREATE KEY RANGE krid2 FROM 11 ROUTE TO sh2;

\c regress

CREATE TABLE test(id int, age int);
INSERT INTO test(id, age) VALUES (10, 16) /*__spqr__sharding_key: 30*/;
INSERT INTO test(id, age) VALUES (10, 16) /*__spqr__sharding_key: 3000*/;