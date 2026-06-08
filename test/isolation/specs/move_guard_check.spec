



# The most basic "REDISTRIBUTE KEY RANGE" behaviour test

setup
{
    select __spqr__console_execute('CREATE DISTRIBUTION d COLUMN TYPES INT; CREATE DISTRIBUTED RELATION r (i) IN d;CREATE KEY RANGE k3 FROM 300 ROUTE TO sh4;CREATE KEY RANGE k2 FROM 200 ROUTE TO sh3;CREATE KEY RANGE k1 FROM 100 ROUTE TO sh2;CREATE KEY RANGE k0 FROM 0 ROUTE TO sh1;');
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
step s1_i              { INSERT INTO r (i, c) VALUES (11, 1); }
step s1_s              { SELECT * FROM r WHERE i = 11 }
step s1_spqr_ctid      { SELECT __spqr__ctid('r'); }


session s2
step s2_redistribute_sh2_nw  { select __spqr__console_execute('REDISTRIBUTE KEY RANGE k0 TO sh2 TASK GROUP zid NOWAIT') /*__spqr__preferred_engine: v2 */; }
step s2_show_tg              { select __spqr__console_execute('SHOW task_groups(task_group_id, destination_shard_id, source_key_range_id, state);') /*__spqr__preferred_engine: v2 */; }
step s2_await_planning       { SELECT pg_sleep(10) /* __spqr__execute_on: sh1 */; }
step s2_await_task           { SELECT __spqr__await_task('zid') /* __spqr__preferred_engine: v2 */; }
step s2_show_kr              { select __spqr__console_execute('SHOW key_ranges(shard_id, locked);'); }

session s3
step s3_clean             { select __spqr__console_execute('drop distribution all cascade');}
step s3_clean_tg          { /* TODO: fix */ select __spqr__console_execute('drop task group zid');}

session s4
step s4_attach_cp            { select __spqr__console_execute('ATTACH CONTROL POINT after_move_keys_cp WAIT;')}
step s4_detach_cp            { select __spqr__console_execute('DETACH CONTROL POINT after_move_keys_cp;')}


# ok
permutation s1_report s1_ev2 s1_i s1_spqr_ctid
            s4_attach_cp s2_redistribute_sh2_nw s2_await_planning
            s2_show_kr s2_show_tg s1_spqr_ctid
            s1_s
            s4_detach_cp
            s2_await_task
            s3_clean s3_clean_tg
