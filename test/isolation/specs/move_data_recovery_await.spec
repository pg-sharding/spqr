
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
    select __spqr__console_execute('CREATE KEY RANGE k33 FROM 300 ROUTE TO sh4;') /*__spqr__preferred_engine: v2 */;
}

setup 
{
    select __spqr__console_execute('CREATE KEY RANGE k22 FROM 200 ROUTE TO sh3;') /*__spqr__preferred_engine: v2 */;
}

setup
{
    select __spqr__console_execute('CREATE KEY RANGE k11 FROM 100 ROUTE TO sh2;') /*__spqr__preferred_engine: v2 */;
}

setup
{
    select __spqr__console_execute('CREATE KEY RANGE k00 FROM 0 ROUTE TO sh1;') /*__spqr__preferred_engine: v2 */;
}

setup
{
    CREATE TABLE r (i INTEGER, c INTEGER);
}

teardown
{
    DROP TABLE r /* __spqr__scatter_query: true */;
}


session s1
step s1_report         { SET __spqr__reply_notice TO on; }
step s1_ev2            { SET __spqr__engine_v2 TO on; }
step s1_begin          { BEGIN; }
step s1_i              { INSERT INTO r (i, c) VALUES (11, 1); }
step s1_spqr_ctid      { SELECT __spqr__ctid('r'); }
step s1_rollback       { ROLLBACK; }


session s2
step s2_redistribute_sh2_nw  { select __spqr__console_execute('REDISTRIBUTE KEY RANGE k00 TO sh2 TASK GROUP zid_await_fails NOWAIT') /*__spqr__preferred_engine: v2 */; }
step s2_show_tg              { select __spqr__console_execute('SHOW task_groups(task_group_id, state, message);') /*__spqr__preferred_engine: v2 */; }
step s2_await_planning       { SELECT pg_sleep(10) /* __spqr__execute_on: sh1 */; }
step s2_await_task           { SELECT __spqr__await_task('zid_await_fails') /* __spqr__preferred_engine: v2 */; }
step s2_show_kr              { select __spqr__console_execute('SHOW key_ranges(shard_id, distribution_id, lower_bound, locked);') /*__spqr__preferred_engine: v2 */; }

session s3
step s3_clean             { select __spqr__console_execute('drop distribution all cascade') /*__spqr__preferred_engine: v2 */;}
step s3_clean_tg          { /* TODO: fix */ select __spqr__console_execute('drop task group zid_await_fails') /*__spqr__preferred_engine: v2 */;}


# ok
permutation 
    s1_report s1_ev2 
    s1_i s1_i s1_i s1_spqr_ctid 
    s1_begin s1_i
    s2_redistribute_sh2_nw s2_await_planning s2_show_kr s2_show_tg 
    s2_await_task
    s2_show_kr s2_show_tg 
    s1_rollback
    s3_clean s3_clean_tg
