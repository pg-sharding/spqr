\c spqr-console

ADD SHARDING RULE t1 COLUMNS id;
ADD KEY RANGE krid1 FROM 1 TO 11 ROUTE TO sh1;
ADD KEY RANGE krid2 FROM 11 TO 101 ROUTE TO sh2;

\c regress

CREATE TABLE test(id int, age int);
INSERT INTO test(id, age) VALUES (10, 16) /*__spqr__sharding_key: 30*/;
INSERT INTO test(id, age) VALUES (10, 16) /*__spqr__sharding_key: 3000*/;