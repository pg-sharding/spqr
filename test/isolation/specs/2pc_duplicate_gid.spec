session s1
step s1_engine    { SET __spqr__engine_v2 TO on; }
step s1_strategy  { SET __spqr__commit_strategy TO '2pc'; }
step s1_cleanup   { SET __spqr__eager_cleanup_2pc TO on; }
step s1_gid       { SELECT __spqr__set_next_2pc_gid('duplicate_2pc_gid'); }
step s1_begin     { BEGIN; }
step s1_query     { SELECT 1+1 /* __spqr__scatter_query: true */; }
step s1_commit    { COMMIT; }

session s2
step s2_engine    { SET __spqr__engine_v2 TO on; }
step s2_strategy  { SET __spqr__commit_strategy TO '2pc'; }
step s2_cleanup   { SET __spqr__eager_cleanup_2pc TO on; }
step s2_gid       { SELECT __spqr__set_next_2pc_gid('duplicate_2pc_gid'); }
step s2_attach    { SELECT __spqr__console_execute('ATTACH CONTROL POINT 2pc_before_prepare_cp WAIT'); }
step s2_begin     { BEGIN; }
step s2_query     { SELECT 1+1 /* __spqr__scatter_query: true */; }
step s2_commit    { COMMIT; }
step s2_rollback  { ROLLBACK; }
step s2_detach    { SELECT __spqr__console_execute('DETACH CONTROL POINT 2pc_before_prepare_cp'); }

permutation
    s1_engine s2_engine s1_strategy s2_strategy s1_cleanup s2_cleanup
    s1_gid s2_gid s2_attach
    s1_begin s1_query s2_begin s2_query
    s1_commit s2_commit s2_rollback s2_detach
