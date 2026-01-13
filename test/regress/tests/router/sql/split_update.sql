
\c spqr-console

CREATE DISTRIBUTION d COLUMN TYPES INT;

CREATE RELATION r (i);

CREATE KEY RANGE k4 FROM 300 ROUTE TO sh4;
CREATE KEY RANGE k3 FROM 200 ROUTE TO sh3;
CREATE KEY RANGE k2 FROM 100 ROUTE TO sh2;
CREATE KEY RANGE k1 FROM 0 ROUTE TO sh1;

\c regress

CREATE TABLE r(i INT, j INT, t TEXT);

COPY r (i, j, t) FROM STDIN DELIMITER '|';
0|10|aaaa
10|11|bbbb
100|101|cccc
200|201|dddd
300|301|eeee
150|151|yyyyy
\.

SET __spqr__allow_split_update TO off;

UPDATE r SET i = 110 WHERE i = 10;
UPDATE r SET i = 110 WHERE i = 100;

SET __spqr__engine_v2 TO on;
SET __spqr__allow_split_update TO on;
SELECT * FROM r /* __spqr__execute_on: sh2 */;
SELECT * FROM r /* __spqr__execute_on: sh3 */;

UPDATE r SET i = 201 WHERE i = 110;

SELECT * FROM r /* __spqr__execute_on: sh2 */;
SELECT * FROM r /* __spqr__execute_on: sh3 */;

BEGIN;

SELECT * FROM r /* __spqr__execute_on: sh1 */;
SELECT * FROM r /* __spqr__execute_on: sh4 */;

UPDATE r SET i = 401 WHERE i = 10;

SELECT * FROM r /* __spqr__execute_on: sh1 */;
SELECT * FROM r /* __spqr__execute_on: sh4 */;

COMMIT;


DROP TABLE r;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;