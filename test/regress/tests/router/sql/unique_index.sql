\c spqr-console
CREATE DISTRIBUTION d COLUMN TYPES INT HASH;

CREATE RELATION r (i HASH murmur);
CREATE RELATION r2 (i HASH murmur);
CREATE RELATION r3 (i HASH murmur);

CREATE UNIQUE INDEX ui ON r COLUMNS (j integer);

CREATE UNIQUE INDEX ui1 ON r2 COLUMNS (j integer);
CREATE UNIQUE INDEX ui2 ON r2 COLUMNS (k integer);

CREATE UNIQUE INDEX ui3 ON r3 COLUMNS (j integer, k integer);

CREATE KEY RANGE k4 FROM 3221225472 ROUTE TO sh4;
CREATE KEY RANGE k3 FROM 2147483648 ROUTE TO sh3;
CREATE KEY RANGE k2 FROM 1073741824 ROUTE TO sh2;
CREATE KEY RANGE k1 FROM 0 ROUTE TO sh1;

\c regress

CREATE TABLE r(i int, j int, k int);

-- unique index is actually a reverse index (just another table)
CREATE TABLE ui(j int);
CREATE UNIQUE INDEX ON ui USING btree(j);

CREATE TABLE r2(i int, j int, k int);

-- unique index is actually a reverse index (just another table)
CREATE TABLE ui1(j int);
CREATE UNIQUE INDEX ON ui1 USING btree(j);

CREATE TABLE ui2(k int);
CREATE UNIQUE INDEX ON ui2 USING btree(k);

CREATE TABLE r3(i int, j int, k int);

-- unique index is actually a reverse index (just another table)
CREATE TABLE ui3(j int, k int);
CREATE UNIQUE INDEX ON ui3 USING btree(j, k);

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

WITH s AS (select 1)
INSERT INTO r (i, j, k) VALUES(10, 2, 30);

INSERT INTO r (i, j, k) VALUES(7, 8, 9) RETURNING *;

DELETE FROM r WHERE i = 7;

UPDATE r SET k = k + 1 WHERE i = 7;

-- test with tx block

BEGIN;

INSERT INTO r (i, j, k) VALUES(7, 8, 9);

SELECT __spqr__ctid('r');
SELECT __spqr__ctid('ui');

ROLLBACK;

SELECT __spqr__ctid('r');
SELECT __spqr__ctid('ui');


-- test multiple index

SELECT __spqr__ctid('r2');
SELECT __spqr__ctid('ui1');
SELECT __spqr__ctid('ui2');

INSERT INTO r2 (i, j, k) VALUES(1, 2, 3);

SELECT __spqr__ctid('r2');
SELECT __spqr__ctid('ui1');
SELECT __spqr__ctid('ui2');


-- should fail
INSERT INTO r2 (i, j, k) VALUES(1, 2, 3);

INSERT INTO r2 (i, j, k) VALUES(4, 5, 6);

SELECT __spqr__ctid('r2');
SELECT __spqr__ctid('ui1');
SELECT __spqr__ctid('ui2');


-- test multi-column index

SELECT __spqr__ctid('r3');
SELECT __spqr__ctid('ui3');

INSERT INTO r3 (i, j, k) VALUES (1, 2, 3);

INSERT INTO r3 (i, j, k) VALUES (2, 3, 2);

SELECT __spqr__ctid('r3');
SELECT __spqr__ctid('ui3');

-- should fail
INSERT INTO r3 (i, j, k) VALUES (1, 2, 3);

-- should fail
INSERT INTO r3 (i, j, k) VALUES (1, 3, 2);

INSERT INTO r3 (i, j, k) VALUES (1, 2, 2);

INSERT INTO r3 (i, j, k) VALUES (1, 3, 3);

SELECT __spqr__ctid('r3');
SELECT __spqr__ctid('ui3');

DROP TABLE r;
DROP TABLE ui;

DROP TABLE r2;
DROP TABLE ui1;
DROP TABLE ui2;

DROP TABLE r3;
DROP TABLE ui3;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;