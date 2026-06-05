
setup
{
    select __spqr__console_execute('CREATE DISTRIBUTION d COLUMN TYPES INT') /*__spqr__preferred_engine: v2 */;
}

setup
{
    select __spqr__console_execute('CREATE DISTRIBUTED RELATION test_table (id) IN d') /*__spqr__preferred_engine: v2 */;
}

setup
{
    select __spqr__console_execute('CREATE REFERENCE TABLE ref_table;') /*__spqr__preferred_engine: v2 */;
}

setup
{
    select __spqr__console_execute('DROP SHARD sh2');
}

teardown
{
    DROP TABLE IF EXISTS test_table;
    DROP TABLE IF EXISTS ref_table;
}

session s1
step disable_engine_v2 { SET __spqr__engine_v2 TO on; }
step create_shard { select __spqr__console_execute('CREATE SHARD sh2 OPTIONS (HOST "spqr_shard_2:6432", HOST "spqr_shard_2_replica:6432", USER "regress", DBNAME "regress")'); }
step create_shard_no_creds { select __spqr__console_execute('CREATE SHARD sh2 OPTIONS (HOST "spqr_shard_2:6432", HOST "spqr_shard_2_replica:6432")'); }
step create_ref_table { CREATE TABLE ref_table(ref_id int) /*__spqr__execute_on: sh2 */; }
step create_table_no_id { CREATE TABLE test_table(name text) /*__spqr__execute_on: sh2 */; }
step create_table_ok { CREATE TABLE test_table(id int, name text) /*__spqr__execute_on: sh2 */; }

session s2
step s2_clean             { select __spqr__console_execute('drop distribution all cascade') /*__spqr__preferred_engine: v2 */;}

permutation disable_engine_v2 create_ref_table create_shard s2_clean
permutation disable_engine_v2 create_ref_table create_shard_no_creds s2_clean
permutation disable_engine_v2 create_ref_table create_table_no_id create_shard s2_clean
permutation disable_engine_v2 create_ref_table create_table_ok create_shard s2_clean
