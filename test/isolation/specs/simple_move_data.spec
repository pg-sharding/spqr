
# The most basic "REDISTRIBUTE KEY RANGE" behaviour test

setup
{
    select __spqr__console_execute('REGISTER ROUTER r1 ADDRESS "[regress_router]:7000"') /*__spqr__preferred_engine: v2 */;
}

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
step s1_i              { INSERT INTO r (i, c) VALUES (11, 2); }
step s1_spqr_ctid      { SELECT __spqr__ctid('r'); }
step s1_commit         { COMMIT; }


session s2
step s2_redistribute_sh2  { select __spqr__console_execute('REDISTRIBUTE KEY RANGE k0 TO sh2') /*__spqr__preferred_engine: v2 */; }
step s2_redistribute_sh1  { select __spqr__console_execute('REDISTRIBUTE KEY RANGE k0 TO sh1') /*__spqr__preferred_engine: v2 */; }
step s2_show_tg           { select __spqr__console_execute('SHOW task_groups;') /*__spqr__preferred_engine: v2 */; }
step s2_show_kr           { select __spqr__console_execute('SHOW key_ranges;') /*__spqr__preferred_engine: v2 */; }

session s3
step s3_clean             { select __spqr__console_execute('drop distribution all cascade') /*__spqr__preferred_engine: v2 */;}

# ok
permutation s1_report s1_ev2 s1_i s1_spqr_ctid s2_redistribute_sh2 s2_show_tg s2_show_kr s1_spqr_ctid s2_show_tg s3_clean
#permutation s1_report s1_ev2 s1_i s1_s s1_begin s1_u s1_commit s2_redistribute s1_spqr_ctid s3_clean
