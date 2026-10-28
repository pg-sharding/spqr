

session s1
step s1_ev2            { SET __spqr__engine_v2 TO on; }
step s1_begin          { BEGIN; }
step s1_adv_lock       { SELECT pg_advisory_lock(2); }
step s1_adv_unlock     { SELECT pg_advisory_unlock(2); }
step s1_rollback       { ROLLBACK; }

session s2
step s2_ev2                { SET __spqr__engine_v2 TO on; }
step s2_try_lock_sh1       { SELECT pg_try_advisory_lock(2) /* __spqr__execute_on: sh1 */; }
step s2_try_lock_sh2       { SELECT pg_try_advisory_lock(2) /* __spqr__execute_on: sh2 */; }
step s2_try_lock_sh3       { SELECT pg_try_advisory_lock(2) /* __spqr__execute_on: sh3 */; }
step s2_try_lock_sh4       { SELECT pg_try_advisory_lock(2) /* __spqr__execute_on: sh4 */; }
step s2_adv_unlock         { SELECT pg_advisory_unlock(2); }

permutation
    s1_ev2 s2_ev2
    s1_adv_lock
    s2_try_lock_sh1 s2_try_lock_sh2 s2_try_lock_sh3 s2_try_lock_sh4
    s1_adv_unlock
    s2_try_lock_sh1 s2_try_lock_sh2 s2_try_lock_sh3 s2_try_lock_sh4
    s2_adv_unlock


permutation
    s1_ev2 s2_ev2
    s1_begin
    s1_adv_lock
    s2_try_lock_sh1 s2_try_lock_sh2 s2_try_lock_sh3 s2_try_lock_sh4
    s1_rollback
    s2_try_lock_sh1 s2_try_lock_sh2 s2_try_lock_sh3 s2_try_lock_sh4
    s2_adv_unlock
    s1_adv_unlock
    s2_try_lock_sh1 s2_try_lock_sh2 s2_try_lock_sh3 s2_try_lock_sh4
    s2_adv_unlock