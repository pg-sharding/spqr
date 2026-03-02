package datatransfers

import (
	"fmt"
	"strings"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/router/rfqn"
)

const CalculateSplitBounds = `
WITH 
sub as (
    SELECT %s, row_number() OVER(ORDER BY %s) as row_n
    FROM (
        SELECT * FROM %s
        WHERE %s
		ORDER BY %s
        LIMIT %d
        OFFSET %d
    ) AS t
),
constants AS (
    SELECT %d as row_count, %d as batch_size
),
max_row AS (
    SELECT count(1) as row_n
    FROM sub
),
total_rows AS (
	SELECT count(1)
	FROM %s
	WHERE %s
)
SELECT DISTINCT ON (%s) sub.*, total_rows.count <= constants.row_count
FROM sub JOIN max_row ON true JOIN constants ON true JOIN total_rows ON true
WHERE (sub.row_n %% constants.batch_size = 0 AND sub.row_n < constants.row_count)
   OR (sub.row_n = constants.row_count)
   OR (max_row.row_n < constants.row_count AND sub.row_n = max_row.row_n)
ORDER BY (%s) %s;
`

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
language plpgsql;`, spqrTransferApplicationName, config.CoordinatorConfig().DataMoveAwaitPIDException)
}

func checkColumnExistsQuery(relation *rfqn.RelationFQN, colName string) string {
	return fmt.Sprintf(`
	SELECT
		count(*) > 0 as column_exists
	FROM information_schema.columns
	WHERE table_name = '%s' AND table_schema = '%s' AND column_name = '%s'`, strings.ToLower(relation.RelationName), strings.ToLower(relation.GetSchema()), colName)
}

func checkConstraintsQuery(dsRelOids, rpRelsClause string) string {
	return fmt.Sprintf(`
	SELECT
		conname
	FROM pg_constraint
	WHERE conrelid IN (%s) and confrelid != 0 and (condeferrable=false or not (confrelid IN (%s)))%s LIMIT 1`, dsRelOids, dsRelOids, rpRelsClause)
}
