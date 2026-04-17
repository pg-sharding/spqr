
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
step s1_ddl                 { select __spqr__remote_execute('host=regress_router_2 port=6432 user=regress dbname=spqr-console', 'SET __spqr__engine_v2 TO on; SET __spqr__commit_strategy TO 2pc; BEGIN; ALTER TABLE r_2pc_aux ADD COLUMN z INT; COMMIT;') /*__spqr__preferred_engine: v2 */; }
step s1_attach_cp           { select __spqr__remote_execute('host=regress_router_2 port=6432 user=regress dbname=spqr-console', 'ATTACH CONTROL POINT 2pc_decision_cp PANIC') /*__spqr__preferred_engine: v2 */; }
step s1_attach_after_cp     { select __spqr__remote_execute('host=regress_router_2 port=6432 user=regress dbname=spqr-console', 'ATTACH CONTROL POINT 2pc_after_decision_cp PANIC') /*__spqr__preferred_engine: v2 */; }
step s1_show_2pc_tx         { select __spqr__console_execute('SHOW two_phase_tx(status);'); }
step s1_run_2pc_recovery    { select __spqr__run_2pc_recover(); }

session s2
step s2_clean             { select __spqr__console_execute('drop distribution all cascade') /*__spqr__preferred_engine: v2 */;}


permutation 
    s1_attach_cp
    s1_ddl
    s1_show_2pc_tx
    s1_run_2pc_recovery
    s1_show_2pc_tx
    s2_clean

permutation 
    s1_attach_after_cp
    s1_ddl
    s1_show_2pc_tx
    s1_run_2pc_recovery
    s1_show_2pc_tx
    s2_clean
