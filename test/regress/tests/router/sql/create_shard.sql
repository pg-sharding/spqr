\c spqr-console

CREATE DISTRIBUTION d COLUMN TYPES INT;
CREATE DISTRIBUTED RELATION test_table (id) IN d;
CREATE REFERENCE TABLE ref_table;
DROP SHARD sh2;

\c regress
SET __spqr__engine_v2 TO on;
CREATE TABLE ref_table(ref_id int) /* __spqr__execute_on: sh2 */;

\c spqr-console
CREATE SHARD sh2 OPTIONS (HOST "spqr_shard_2:6432", HOST "spqr_shard_2_replica:6432", USER "regress", DBNAME "regress");

CREATE SHARD sh2 OPTIONS (HOST "spqr_shard_2:6432", HOST "spqr_shard_2_replica:6432");

\c regress

CREATE TABLE test_table(name text) /*__spqr__execute_on: sh2 */;

\c spqr-console
CREATE SHARD sh2 OPTIONS (HOST "spqr_shard_2:6432", HOST "spqr_shard_2_replica:6432", USER "regress", DBNAME "regress");

\c regress
DROP TABLE test_table /*__spqr__execute_on: sh2 */;
CREATE TABLE test_table(id int, name text) /*__spqr__execute_on: sh2 */;

\c spqr-console
CREATE SHARD sh2 OPTIONS (HOST "spqr_shard_2:6432", HOST "spqr_shard_2_replica:6432", USER "regress", DBNAME "regress");

DROP DISTRIBUTION ALL CASCADE;
