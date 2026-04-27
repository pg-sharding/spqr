
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
    select __spqr__console_execute('CREATE KEY RANGE k3 FROM 300 ROUTE TO sh4;') /*__spqr__preferred_engine: v2 */;
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

teardown
{
    DROP TABLE r;
}


session s1
step s1_report         { SET __spqr__reply_notice TO on; }
step s1_ev2            { SET __spqr__engine_v2 TO on; }
step s1_begin          { BEGIN; }
step s1_i              { INSERT INTO r (i, c) VALUES (11, 1); }
step s1_spqr_ctid      { SELECT __spqr__ctid('r'); }
step s1_commit         { COMMIT; }


session s2
step s2_redistribute_sh2_nw     { select __spqr__console_execute('REDISTRIBUTE KEY RANGE k0 TO sh2 TASK GROUP zid NOWAIT') /*__spqr__preferred_engine: v2 */; }
step s2_await_task           { SELECT __spqr__await_task('zid') /* __spqr__preferred_engine: v2 */; }
step s2_show_kr              { select __spqr__console_execute('SHOW key_ranges;') /*__spqr__preferred_engine: v2 */; }


session s3
step s3_clean             { select __spqr__console_execute('drop distribution all cascade') /*__spqr__preferred_engine: v2 */;}
step s3_clean_lock        { /* TODO: fix */ select __spqr__console_execute('UNLOCK KEY RANGE ALL')}

session s4
step s4_cancel             { select __spqr__console_execute('STOP TASK GROUP zid') /*__spqr__preferred_engine: v2 */;}
step s4_show_tg            { select __spqr__console_execute('SHOW task_groups(task_group_id, source_key_range_id, state, message);') /*__spqr__preferred_engine: v2 */; }
step s4_await_planning     { SELECT pg_sleep(10) /* __spqr__execute_on: sh1 */; }


# ok
permutation 
    s1_report s1_ev2 s1_i 
    s1_spqr_ctid s1_begin s1_i
    s2_redistribute_sh2_nw
    s4_await_planning
    s4_show_tg
    s1_spqr_ctid
    s4_cancel
    s2_await_task
    s4_show_tg
    s1_spqr_ctid
    s1_commit

    s3_clean_lock
    s3_clean

