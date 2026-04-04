
# The most basic "REDISTRIBUTE KEY RANGE" behaviour test

setup
{
    select __spqr__console_execute('CREATE DISTRIBUTION d COLUMN TYPES INT') /*__spqr__preferred_engine: v2 */;
}

setup
{
    select __spqr__console_execute('CREATE DISTRIBUTED RELATION r_2pc_aux (i) IN d') /*__spqr__preferred_engine: v2 */;
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
    CREATE TABLE r_2pc_aux (i INTEGER, c INTEGER);
}

teardown
{
    DROP TABLE r_2pc_aux;
}


session s1
step s1_report         { SET __spqr__reply_notice TO on; }
step s1_ev2            { SET __spqr__engine_v2 TO on; }
step s1_c_str          { SET __spqr__commit_strategy TO 2pc; }
step s1_begin          { BEGIN; }
step s1_ddl            { ALTER TABLE r_2pc_aux ADD COLUMN z INT; }
step s1_commit         { COMMIT; }


session s2
step s2_attach_cp           { select __spqr__console_execute('ATTACH CONTROL POINT 2pc_decision_cp wait') /*__spqr__preferred_engine: v2 */; }
step s2_attach_after_cp     { select __spqr__console_execute('ATTACH CONTROL POINT 2pc_after_decision_cp wait') /*__spqr__preferred_engine: v2 */; }
step s2_detach_cp          { select __spqr__console_execute('DETACH CONTROL POINT 2pc_decision_cp') /*__spqr__preferred_engine: v2 */; }
step s2_detach_after_cp    { select __spqr__console_execute('DETACH CONTROL POINT 2pc_after_decision_cp') /*__spqr__preferred_engine: v2 */; }

step s2_show_2pc_tx              { select __spqr__console_execute('SHOW two_phase_tx(status);'); }


session s3
step s3_clean             { select __spqr__console_execute('drop distribution all cascade') /*__spqr__preferred_engine: v2 */;}


permutation 
    s1_report s1_ev2 s1_c_str
    s1_begin
    s2_attach_cp s2_attach_after_cp
    s2_show_2pc_tx
    s1_ddl
    s1_commit
    s2_show_2pc_tx
    s2_detach_cp
    s2_show_2pc_tx
    s2_detach_after_cp
    s2_show_2pc_tx
    s3_clean