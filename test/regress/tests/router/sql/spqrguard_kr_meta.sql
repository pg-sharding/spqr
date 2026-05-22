\c spqr-console
CREATE DISTRIBUTION ds1 (INT);
CREATE KEY RANGE kr1 FROM 0 ROUTE TO sh1;

\c regress
SET __spqr__execute_on=sh1;
SELECT * FROM spqr_metadata.spqr_local_key_ranges;
SET __spqr__execute_on=sh2;
SELECT * FROM spqr_metadata.spqr_local_key_ranges;
CREATE TABLE t (id int);
INSERT INTO t (id) VALUES (1);
SET __spqr__execute_on=sh1;
SELECT * FROM spqr_metadata.spqr_local_key_ranges;
SET __spqr__execute_on=sh2;
SELECT * FROM spqr_metadata.spqr_local_key_ranges;

\c spqr-console
MOVE KEY RANGE kr1 TO sh2;

\c regress
SET __spqr__execute_on=sh1;
SELECT * FROM spqr_metadata.spqr_local_key_ranges;
SET __spqr__execute_on=sh2;
SELECT * FROM spqr_metadata.spqr_local_key_ranges;

\c spqr-console
SPLIT KEY RANGE kr2 FROM kr1 BY 10;

\c regress
SET __spqr__execute_on=sh1;
SELECT * FROM spqr_metadata.spqr_local_key_ranges;
SET __spqr__execute_on=sh2;
SELECT * FROM spqr_metadata.spqr_local_key_ranges;

\c spqr-console
UNITE KEY RANGE kr1 WITH kr2;

\c regress
SET __spqr__execute_on=sh1;
SELECT * FROM spqr_metadata.spqr_local_key_ranges;
SET __spqr__execute_on=sh2;
SELECT * FROM spqr_metadata.spqr_local_key_ranges;

\c spqr-console
DROP KEY RANGE kr1;

\c regress
SET __spqr__execute_on=sh1;
SELECT * FROM spqr_metadata.spqr_local_key_ranges;
SET __spqr__execute_on=sh2;
SELECT * FROM spqr_metadata.spqr_local_key_ranges;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;