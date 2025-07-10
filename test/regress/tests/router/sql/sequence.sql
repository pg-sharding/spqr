\c spqr-console

CREATE REFERENCE TABLE test AUTO INCREMENT id;
CREATE REFERENCE TABLE test2 AUTO INCREMENT id START 19;

SHOW sequences;

\c regress

CREATE TABLE test(id int, age int);
CREATE TABLE test2(id int, age int);


INSERT INTO test(age) VALUES (16) /* __spqr__engine_v2: true */;
INSERT INTO test(age) VALUES (17) /* __spqr__engine_v2: true */;
INSERT INTO test(age) VALUES (18) /* __spqr__engine_v2: true */;

SELECT * FROM test /* __spqr__execute_on: sh2 */ ORDER BY id, age;

INSERT INTO test2(age) VALUES (16) /* __spqr__engine_v2: true */;
INSERT INTO test2(age) VALUES (17) /* __spqr__engine_v2: true */;
INSERT INTO test2(age) VALUES (18) /* __spqr__engine_v2: true */;

-- asserting error
INSERT INTO test2 SELECT 1 /* __spqr__engine_v2: true */;

SELECT * FROM test2 /* __spqr__execute_on: sh2 */ ORDER BY id, age;

DROP TABLE test;
DROP TABLE test2;

\c spqr-console

DROP REFERENCE RELATION test;
DROP REFERENCE RELATION test2;

SHOW sequences;

DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;
DROP SEQUENCE test_id;
DROP SEQUENCE test2_id;
