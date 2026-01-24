# The most basic "LOCK KEY RANGE" behaviour test


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
    CREATE TABLE r (i INTEGER);
}

teardown
{
    DROP TABLE r;
}


session s1
step s1_report         { SET __spqr__reply_notice TO on; }
step s1_ev2            { SET __spqr__engine_v2 TO on; }
step s1_begin          { BEGIN; }
step s1_s              { SELECT * FROM r WHERE i = 11; }
step s1_commit         { COMMIT; }


session s2
step s2_lock           { select __spqr__console_execute('LOCK KEY RANGE k0') /*__spqr__preferred_engine: v2 */; }
step s2_unlock         { select __spqr__console_execute('UNLOCK KEY RANGE k0') /*__spqr__preferred_engine: v2 */; }

session s3
step s3_clean             { select __spqr__console_execute('drop distribution all cascade') /*__spqr__preferred_engine: v2 */;}

# ok
permutation s1_report s1_ev2 s1_begin s1_s s1_s s1_commit s3_clean
permutation s1_report s1_ev2 s2_lock s1_begin s2_unlock s1_s s1_s s1_commit s3_clean


# should fail
permutation s1_report s1_ev2 s1_begin s1_s s2_lock s1_s s1_commit s2_unlock s3_clean
permutation s2_lock s1_report s1_ev2 s1_begin s1_s s1_s s1_commit s2_unlock s3_clean