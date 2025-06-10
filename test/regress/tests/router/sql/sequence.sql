\c spqr-console

CREATE REFERENCE TABLE test AUTO INCREMENT id;

\c regress

CREATE TABLE test(id int, age int);
INSERT INTO test(age) VALUES (16) /* __spqr__engine_v2: true */;
INSERT INTO test(age) VALUES (17) /* __spqr__engine_v2: true */;
INSERT INTO test(age) VALUES (18) /* __spqr__engine_v2: true */;

SELECT * FROM test /* __spqr__execute_on: sh2 */;

\c spqr-console

DROP REFERENCE RELATION test;

DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;
DROP SEQUENCE test_id;
