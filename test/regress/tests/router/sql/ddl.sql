\c spqr-console

CREATE DISTRIBUTION ds1 COLUMN TYPES integer;

CREATE KEY RANGE krid1 FROM 1 ROUTE TO sh1 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid2 FROM 101 ROUTE TO sh2 FOR DISTRIBUTION ds1;

ALTER DISTRIBUTION ds1 ATTACH RELATION table_1 DISTRIBUTION KEY id;
ALTER DISTRIBUTION ds1 ATTACH RELATION table_2 DISTRIBUTION KEY order_id;

\c regress

CREATE TABLE table_1(id INT PRIMARY KEY);
CREATE TABLE table_2(id INT PRIMARY KEY);

BEGIN;
ALTER TABLE "table_1" RENAME TO "tmp";
ALTER TABLE "table_2" RENAME TO "table_1";
ALTER TABLE "tmp" RENAME TO "table_2";
COMMIT;

DROP TABLE table_1 CASCADE;
DROP TABLE table_2;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;