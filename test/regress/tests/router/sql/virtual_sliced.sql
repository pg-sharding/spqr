
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


SELECT __spqr__ctid('r');

-- should be rejected.

SELECT 1, __spqr__ctid('r');

WITH s as (SELECT 1) SELECT __spqr__ctid('r');

SELECT __spqr__ctid('r') UNION ALL SELECT __spqr__ctid('r');

SELECT  __spqr__ctid('r'), now();

DROP TABLE r;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;