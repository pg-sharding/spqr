
# The most basic "REDISTRIBUTE KEY RANGE" behaviour test

setup
{
    select __spqr__console_execute('CREATE DISTRIBUTION d COLUMN TYPES INT') /*__spqr__preferred_engine: v2 */;
}

setup
{
    select __spqr__console_execute('CREATE DISTRIBUTED RELATION sh1.r (i) IN d') /*__spqr__preferred_engine: v2 */;
}

setup
{
    select __spqr__console_execute('CREATE DISTRIBUTED RELATION sh2.r (i) IN d') /*__spqr__preferred_engine: v2 */;
}

setup
{
    select __spqr__console_execute('CREATE DISTRIBUTED RELATION sh3.r (j) IN d') /*__spqr__preferred_engine: v2 */;
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
    CREATE SCHEMA sh1;
    CREATE TABLE sh1.r (i INTEGER, c INTEGER);
    CREATE SCHEMA sh2;
    CREATE TABLE sh2.r (i INTEGER, c INTEGER);
    CREATE SCHEMA sh3;
    CREATE TABLE sh3.r (j INTEGER, c INTEGER);
}

teardown
{
    DROP SCHEMA sh1 CASCADE;
    DROP SCHEMA sh2 CASCADE;
}


session s1
step s1_report             { SET __spqr__reply_notice TO on; }
step s1_ev2                { SET __spqr__engine_v2 TO on; }
step s1_sh1_i              { INSERT INTO sh1.r (i, c) VALUES (11, 1); }
step s1_sh2_i              { INSERT INTO sh2.r (i, c) VALUES (11, 1); }
step s1_sh3_i              { INSERT INTO sh3.r (j, c) VALUES (11, 1); }
step s1_sh1_spqr_ctid      { SELECT __spqr__ctid('sh1.r'); }
step s1_sh2_spqr_ctid      { SELECT __spqr__ctid('sh2.r'); }
step s1_sh3_spqr_ctid      { SELECT __spqr__ctid('sh3.r'); }

session s2
step s2_redistribute_sh2     { select __spqr__console_execute('REDISTRIBUTE KEY RANGE k0 TO sh2') /*__spqr__preferred_engine: v2 */; }
step s2_redistribute_sh1     { select __spqr__console_execute('REDISTRIBUTE KEY RANGE k0 TO sh1') /*__spqr__preferred_engine: v2 */; }
step s2_show_kr              { select __spqr__console_execute('SHOW key_ranges;') /*__spqr__preferred_engine: v2 */; }

session s3
step s3_clean             { select __spqr__console_execute('drop distribution all cascade') /*__spqr__preferred_engine: v2 */;}

# ok
permutation 
    s1_report s1_ev2
    s1_sh1_i s1_sh2_i s1_sh3_i
    s1_sh1_spqr_ctid s1_sh2_spqr_ctid s1_sh3_spqr_ctid
    s2_redistribute_sh2
    s2_show_kr s1_sh1_spqr_ctid s1_sh2_spqr_ctid s1_sh3_spqr_ctid
    s2_redistribute_sh1
    s2_show_kr s1_sh1_spqr_ctid s1_sh2_spqr_ctid s1_sh3_spqr_ctid
    s3_clean