\c spqr-console
CREATE DISTRIBUTION d COLUMN TYPES INT HASH;

CREATE RELATION r (i HASH murmur);

CREATE UNIQUE INDEX ui ON r COLUMN j TYPE integer;

CREATE KEY RANGE k4 FROM 3221225472 ROUTE TO sh4;
CREATE KEY RANGE k3 FROM 2147483648 ROUTE TO sh3;
CREATE KEY RANGE k2 FROM 1073741824 ROUTE TO sh2;
CREATE KEY RANGE k1 FROM 0 ROUTE TO sh1;

\c regress

CREATE TABLE r(i int, j int, k int);

-- unique index is actually a reverse index (just another table)
CREATE TABLE ui(j int);
CREATE UNIQUE INDEX ON ui USING btree(j);

SET __spqr__engine_v2 TO true;

SELECT __spqr__ctid('r');
SELECT __spqr__ctid('ui');


INSERT INTO r (i, j, k) VALUES(1, 2, 3);

SELECT __spqr__ctid('r');
SELECT __spqr__ctid('ui');

INSERT INTO r (i, j, k) VALUES(3, 4, 5);

SELECT __spqr__ctid('r');
SELECT __spqr__ctid('ui');

-- should fail

INSERT INTO r (i, j, k) VALUES(10, 2, 30);

-- test with tx block

BEGIN;

INSERT INTO r (i, j, k) VALUES(7, 8, 9);

SELECT __spqr__ctid('r');
SELECT __spqr__ctid('ui');

ROLLBACK;

SELECT __spqr__ctid('r');
SELECT __spqr__ctid('ui');

DROP TABLE r;
DROP TABLE ui;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;