# A key range created after RENAME has acquired the source operation lock
# must not be overwritten by the rename.

setup
{
    select __spqr__console_execute('CREATE DISTRIBUTION d COLUMN TYPES INT') /*__spqr__preferred_engine: v2 */;
}

setup
{
    select __spqr__console_execute('CREATE KEY RANGE kr_old FROM 100 ROUTE TO sh1') /*__spqr__preferred_engine: v2 */;
}

teardown
{
    select __spqr__console_execute('DROP DISTRIBUTION ALL CASCADE') /*__spqr__preferred_engine: v2 */;
}

session s1
step s1_rename { select __spqr__console_execute('RENAME KEY RANGE kr_old TO kr_new') /*__spqr__preferred_engine: v2 */; }

session s2
step s2_attach_cp { select __spqr__remote_execute('host=regress_coordinator port=7002 user=regress dbname=regress', 'ATTACH CONTROL POINT after_lock_key_range_cp wait') /*__spqr__preferred_engine: v2 */; }
# Give the rename request time to reach the coordinator control point.
step s2_wait_for_rename { SELECT pg_sleep(1) /* __spqr__execute_on: sh1 */; }
step s2_create_new { select __spqr__console_execute('CREATE KEY RANGE kr_new FROM 0 ROUTE TO sh2') /*__spqr__preferred_engine: v2 */; }
step s2_detach_cp { select __spqr__remote_execute('host=regress_coordinator port=7002 user=regress dbname=regress', 'DETACH CONTROL POINT after_lock_key_range_cp') /*__spqr__preferred_engine: v2 */; }
step s2_show_key_ranges { select __spqr__console_execute('SHOW key_ranges(key_range_id, shard_id)') /*__spqr__preferred_engine: v2 */; }

permutation s2_attach_cp s1_rename(*) s2_wait_for_rename s2_create_new s2_detach_cp s2_show_key_ranges
