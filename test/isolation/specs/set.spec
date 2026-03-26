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
step s1_set { SET application_name TO 's1'; }
step s1_show_sh1 { SHOW application_name /* __spqr__execute_on: sh1 */; }
step s1_show_sh2 { SHOW application_name /* __spqr__execute_on: sh2 */; }


session s2
step s2_set { SET application_name TO 's2'; }
step s2_show { SHOW application_name /* __spqr__execute_on: sh1 */; }

session s3
step s3_clean { select __spqr__console_execute('drop distribution all cascade') /*__spqr__preferred_engine: v2 */;}

# ok
permutation s1_set s2_set s1_show_sh1 s2_show s1_show_sh2 s3_clean


# should fail
