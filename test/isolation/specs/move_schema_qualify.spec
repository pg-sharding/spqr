
# The most basic "REDISTRIBUTE KEY RANGE" behaviour test

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
    select __spqr__console_execute('ALTER DISTRIBUTION d ALTER RELATION r SCHEMA sh') /*__spqr__preferred_engine: v2 */;
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
    CREATE SCHEMA sh;
    CREATE TABLE sh.r (i INTEGER, c INTEGER);
}

teardown
{
    DROP SCHEMA sh CASCADE;
}


session s1
step s1_report         { SET __spqr__reply_notice TO on; }
step s1_ev2            { SET __spqr__engine_v2 TO on; }
step s1_i              { INSERT INTO sh.r (i, c) VALUES (11, 1); }
step s1_spqr_ctid      { SELECT __spqr__ctid('sh.r'); }


session s2
step s2_redistribute_sh2     { select __spqr__console_execute('REDISTRIBUTE KEY RANGE k0 TO sh2') /*__spqr__preferred_engine: v2 */; }
step s2_redistribute_sh1     { select __spqr__console_execute('REDISTRIBUTE KEY RANGE k0 TO sh1') /*__spqr__preferred_engine: v2 */; }
step s2_show_kr              { select __spqr__console_execute('SHOW key_ranges;') /*__spqr__preferred_engine: v2 */; }

session s3
step s3_clean             { select __spqr__console_execute('drop distribution all cascade') /*__spqr__preferred_engine: v2 */;}

# ok
permutation s1_report s1_ev2 s1_i s1_spqr_ctid s2_redistribute_sh2 s2_show_kr s1_spqr_ctid s2_redistribute_sh1 s2_show_kr s1_spqr_ctid s3_clean
