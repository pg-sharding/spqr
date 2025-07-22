\c spqr-console

CREATE REFERENCE TABLE test AUTO INCREMENT id;
CREATE REFERENCE TABLE test2 AUTO INCREMENT id START 19;
CREATE REFERENCE TABLE test3 AUTO INCREMENT a START 101;

SHOW sequences;

\c regress

CREATE TABLE test(id int, age int);
CREATE TABLE test2(id int, age int);
CREATE TABLE test3(a INT, b INT, c INT);


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


INSERT INTO test3(b,c) VALUES (1001, 1002),(101, 102), (999, 998) /* __spqr__engine_v2: true */;

SELECT * FROM test3 /* __spqr__execute_on: sh4 */ ORDER BY 1,2,3;

DROP TABLE test;
DROP TABLE test2;
DROP TABLE test3;

\c spqr-console

DROP REFERENCE RELATION test;
DROP REFERENCE RELATION test2;
DROP REFERENCE RELATION test3;

SHOW sequences;

DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;
