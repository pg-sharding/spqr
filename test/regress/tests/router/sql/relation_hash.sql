\c spqr-console
CREATE DISTRIBUTION ds1 COLUMN TYPES int hash;
CREATE RELATION old_composite (murmur [s varchar hash, i int hash]);
ALTER DISTRIBUTION ds1 ATTACH RELATION new_composite
  DISTRIBUTION KEY murmur [s varchar, i uinteger];

CREATE KEY RANGE FROM 3221225472 ROUTE TO sh4;
CREATE KEY RANGE FROM 2147483648 ROUTE TO sh3;
CREATE KEY RANGE FROM 1073741824 ROUTE TO sh2;
CREATE KEY RANGE FROM 0 ROUTE TO sh1;

\c regress
CREATE TABLE old_composite (s varchar, i bigint);
CREATE TABLE new_composite (s varchar, i bigint);

INSERT INTO old_composite (s, i) VALUES ('entity', 0), ('entity', 1), ('entity', 42);
INSERT INTO new_composite (s, i) VALUES ('entity', 0), ('entity', 1), ('entity', 42);
SELECT * FROM old_composite WHERE s = 'entity' AND i = 42;
SELECT * FROM new_composite WHERE s = 'entity' AND i = 42;

DROP TABLE old_composite;
DROP TABLE new_composite;
\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
