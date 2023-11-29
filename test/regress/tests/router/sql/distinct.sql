\c spqr-console

ADD SHARDING RULE t1 COLUMNS id;
ADD KEY RANGE krid1 FROM 1 TO 11 ROUTE TO sh1;
ADD KEY RANGE krid2 FROM 11 TO 101 ROUTE TO sh2;

\c regress

CREATE TABLE test_dist(id int);
INSERT INTO test_dist(id) VALUES(5);
INSERT INTO test_dist(id) VALUES(5);
INSERT INTO test_dist(id) VALUES(5);
INSERT INTO test_dist(id) VALUES(5);
INSERT INTO test_dist(id) VALUES(5);
SELECT id FROM test_dist WHERE id=5;
SELECT DISTINCT id FROM test_dist WHERE id=5;