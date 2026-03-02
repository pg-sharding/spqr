
# The most basic "REDISTRIBUTE KEY RANGE" behaviour test

setup
{
    select __spqr__console_execute('CREATE DISTRIBUTION d COLUMN TYPES INT') /*__spqr__preferred_engine: v2 */;
}

setup
{
    select __spqr__console_execute('CREATE DISTRIBUTED RELATION r (i) IN d') /*__spqr__preferred_engine: v2 */;
}

setup 
{
    select __spqr__console_execute('CREATE KEY RANGE k2 FROM 200 ROUTE TO sh3;') /*__spqr__preferred_engine: v2 */;
}

setup
{
    select __spqr__console_execute('CREATE KEY RANGE k1 FROM 100 ROUTE TO sh2;') /*__spqr__preferred_engine: v2 */;
}

setup
{
    select __spqr__console_execute('CREATE KEY RANGE k0 FROM 0 ROUTE TO sh1;') /*__spqr__preferred_engine: v2 */;
}

setup
{
    CREATE TABLE r (i INTEGER, c INTEGER);
}


session s1
step s1_report         { SET __spqr__reply_notice TO on; }
step s1_ev2            { SET __spqr__engine_v2 TO on; }
step s1_i              { INSERT INTO r (i, c) VALUES (11, 1); }


session s2
step s2_add_shard             { select __spqr__console_execute('ADD SHARD sh4 WITH HOSTS "shard4:6432","shard4_replica:6432" OPTIONS (dbname regress, user regress, password "12345678")') /*__spqr__preferred_engine: v2 */; }
step s2_create_table_on_shard { CREATE TABLE r (i INTEGER, c INTEGER) /* __spqr__execute_on: sh4 */ }
step s2_redistribute_sh4      { select __spqr__console_execute('REDISTRIBUTE KEY RANGE k0 TO sh4') /*__spqr__preferred_engine: v2 */; }
step s2_show_kr               { select __spqr__console_execute('SHOW key_ranges;') /*__spqr__preferred_engine: v2 */; }

session s3
step s3_drop_table        { DROP TABLE r;}
step s3_clean             { select __spqr__console_execute('drop distribution all cascade') /*__spqr__preferred_engine: v2 */;}
step s3_drop_shard        { select __spqr__console_execute('drop shard sh4 cascade') /*__spqr__preferred_engine: v2 */;}


permutation s1_report s1_ev2 s1_i s2_add_shard s2_create_table_on_shard s2_redistribute_sh4 s2_show_kr s3_drop_table s3_clean s3_drop_shard
