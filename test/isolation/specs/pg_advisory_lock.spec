

session s1
step s1_ev2            { SET __spqr__engine_v2 TO on; }
step s1_adv_lock       { SELECT pg_advisory_lock(2); }
step s1_try_lock       { SELECT pg_try_advisory_lock(2); }
step s1_adv_unlock     { SELECT pg_advisory_unlock(2); }
step s1_xact_lock      { SELECT pg_advisory_xact_lock(2); }

permutation
    s1_ev2
    s1_adv_lock
    s1_try_lock
    s1_adv_unlock
    s1_xact_lock
