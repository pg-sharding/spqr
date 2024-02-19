\c spqr-console

CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
CREATE KEY RANGE krid1 FROM 1 ROUTE TO sh1 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid2 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION ds1;
ALTER DISTRIBUTION ds1 ATTACH RELATION test DISTRIBUTION KEY id;

\c regress

CREATE TABLE test(id int, age int);
-- TODO: specify distribution as well as sharding_key
INSERT INTO test(id, age) VALUES (10, 16) /*__spqr__sharding_key: 30*/;
INSERT INTO test(id, age) VALUES (10, 16) /*__spqr__sharding_key: 3000*/;

DROP TABLE test;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;