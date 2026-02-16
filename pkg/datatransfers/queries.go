package datatransfers

import "fmt"

func getAwaitPIDsQuery() string {
	return fmt.Sprintf(`
do $$
declare
	v_pids text;
begin
	SELECT coalesce(array_agg(l.virtualtransaction), '{}') INTO v_pids
	FROM pg_locks AS l 
	LEFT JOIN pg_stat_activity AS a 
	ON l.pid = a.pid 
	LEFT JOIN pg_database AS d 
	ON a.datid = d.oid 
	WHERE l.locktype = 'virtualxid' 
	AND l.pid NOT IN (pg_backend_pid()) 
	AND (l.virtualxid, l.virtualtransaction) <> ('1/1', '-1/0') 
	AND (a.application_name IS NULL OR NOT (a.application_name like '%s%%'))
	AND a.query !~* E'^\\\\s*vacuum\\\\s+' 
	AND a.query !~ E'^autovacuum: ' 
	AND a.query !~ E'repack'
	AND %s
	AND ((d.datname IS NULL OR d.datname = current_database()) OR l.database = 0);

	RAISE NOTICE 'v_pids = %%', v_pids;

	loop
		if (
			SELECT count(pid) FROM pg_locks WHERE locktype = 'virtualxid'
	 			AND pid <> pg_backend_pid() AND virtualtransaction = ANY(v_pids::text[])
		) = 0 then
			return;
		end if;
		perform pg_sleep(0.5);
	end loop;

end;
$$
language plpgsql;`, spqrTransferApplicationName, )
}

func checkColumnExistsQuery(relName, schema, colName string) string {
	return fmt.Sprintf(`SELECT count(*) > 0 as column_exists FROM information_schema.columns WHERE table_name = '%s' AND table_schema = '%s' AND column_name = '%s'`, relName, schema, colName)
}

func checkConstraintsQuery(dsRelOids, rpRelsClause string) string {
	return fmt.Sprintf(`SELECT conname FROM pg_constraint WHERE conrelid IN (%s) and confrelid != 0 and (condeferrable=false or not (confrelid IN (%s)))%s LIMIT 1`, dsRelOids, dsRelOids, rpRelsClause)
}
