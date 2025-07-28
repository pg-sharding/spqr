\c regress

CREATE TABLE copy_test (id int)  /* __spqr__scatter_query: true, __spqr__default_route_behaviour: allow */;

SET __spqr__execute_on TO sh1;

INSERT INTO copy_test VALUES(1);

SELECT id FROM copy_test ORDER BY id;

COPY copy_test(id) FROM STDIN WHERE id <= 10;
1
2
3
4
5
\.

SELECT id FROM copy_test ORDER BY id;


RESET __spqr__execute_on;

\c spqr-console
CREATE DISTRIBUTION ds1 COLUMN TYPES int;
CREATE KEY RANGE krid2 FROM 30 ROUTE TO sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;
CREATE RELATION copy_test(id);
\c regress

SET __spqr__engine_v2 TO off;

BEGIN;

SELECT FROM copy_test WHERE id = 29;

COPY copy_test(id) FROM STDIN;
10
112
113
114
115
\.

COMMIT;

SELECT id FROM copy_test ORDER BY id /* __spqr__execute_on: sh1 */;
SELECT id FROM copy_test ORDER BY id /* __spqr__execute_on: sh2 */;

DROP TABLE IF EXISTS copy_test;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;
