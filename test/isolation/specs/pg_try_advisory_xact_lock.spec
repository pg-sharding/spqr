session s1
step s1_ev2       { SET __spqr__engine_v2 TO on; }
step s1_begin     { BEGIN; }
step s1_lock      { SELECT pg_advisory_xact_lock(1) /* __spqr__execute_on: sh1 */; }
step s1_rollback  { ROLLBACK; }

session s2
step s2_ev2       { SET __spqr__engine_v2 TO on; }
step s2_begin     { BEGIN; }
step s2_try_lock  { SELECT pg_try_advisory_xact_lock(1); }
step s2_rollback  { ROLLBACK; }

permutation
    s1_ev2 s2_ev2
    s1_begin s1_lock
    s2_begin s2_try_lock s2_rollback
    s1_rollback
    s2_begin s2_try_lock s2_rollback
