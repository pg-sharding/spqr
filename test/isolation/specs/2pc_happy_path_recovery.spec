

session s1
step s1_report             { SET __spqr__reply_notice TO on; }
step s1_commit_strategy    { set __spqr__commit_strategy to 2pc; }
step s1_ev2                { set __spqr__engine_v2 to true; }
step s1_begin              { BEGIN; }
step s1_s_m                { SELECT 1+2 INTO z/* __spqr__scatter_query: true */; }
step s1_commit             { COMMIT; }
step s1_curr_set           { select current_setting('twopc_aux_tester.error_on_prepare') /* __spqr__scatter_query: true */; }
step s1_set_conf           { select set_config('twopc_aux_tester.enabled', 'on', false) /* __spqr__execute_on: sh4 */;}
step s1_set_conf2          { select set_config('twopc_aux_tester.error_on_prepare', 'on', false) /* __spqr__execute_on: sh4 */;}
step s1_show_pg_prep_xacts { select __spqr__ctid('pg_prepared_xacts'); }


permutation
    s1_report s1_ev2 s1_commit_strategy
    s1_set_conf s1_set_conf2
    s1_begin s1_curr_set
    s1_s_m
    s1_commit
    s1_commit
    s1_show_pg_prep_xacts