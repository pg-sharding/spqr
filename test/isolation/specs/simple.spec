
setup
{
  select __spqr__console_execute('CREATE DISTRIBUTION d COLUMN TYPES INT') /*__spqr__preferred_engine: v2 */;
  select __spqr__console_execute('CREATE DISTRIBUTED RELATION r (i) IN d') /*__spqr__preferred_engine: v2 */;
}

teardown
{
  select __spqr__console_execute('drop distribution all cascade') /*__spqr__preferred_engine: v2 */;
}


session s1
step s1_report         { SET __spqr__reply_notice TO on; }
step s1_begin          { BEGIN; }
step s1_s     { SELECT 1 /* __spqr__execute_on: sh1 */; }
step s1_commit         { COMMIT; }

permutation s1_report s1_begin s1_s s1_commit
