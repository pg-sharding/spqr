


session s1
step s1_report         { SET __spqr__reply_notice TO on; }
step s1_begin          { BEGIN; }
step s1_s_s            { SELECT 1+2 /* __spqr__execute_on: sh1 */; }
step s1_s_m            { SELECT 1+2 /* __spqr__scatter_query: true */; }
step s1_commit         { COMMIT; }
step s1_rollback       { ROLLBACK; }



session s2
step s2_cancel          { select BOOL_AND(pg_terminate_backend(pid)) from pg_stat_activity  where backend_type = 'client backend' and pid != pg_backend_pid()  /* __spqr__execute_on: sh1 */; } 
step s2_cancel_all      { select BOOL_AND(pg_terminate_backend(pid)) from pg_stat_activity  where backend_type = 'client backend' and pid != pg_backend_pid()  /* __spqr__scatter_query: true */; } 

permutation s1_report s1_s_s s1_begin s1_s_s s2_cancel s1_commit s1_commit
permutation s1_report s1_s_s s1_begin s1_s_s s2_cancel s1_rollback s1_rollback
permutation s1_report s1_s_s s1_begin s1_s_s s2_cancel s1_rollback s1_commit
permutation s1_report s1_s_s s1_begin s1_s_s s2_cancel s1_commit s1_rollback


permutation s1_report s1_s_m s1_begin s1_s_m s2_cancel_all s1_commit s1_commit
permutation s1_report s1_s_m s1_begin s1_s_m s2_cancel_all s1_rollback s1_rollback
permutation s1_report s1_s_m s1_begin s1_s_m s2_cancel_all s1_rollback s1_commit
permutation s1_report s1_s_m s1_begin s1_s_m s2_cancel_all s1_commit s1_rollback