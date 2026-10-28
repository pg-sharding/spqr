\c spqr-console
-- INSERT ... SELECT with nested subselects, computed columns and RETURNING
-- must route by the pulled-up id value
CREATE DISTRIBUTION d (int);
CREATE KEY RANGE k2 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION d;
CREATE KEY RANGE k1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION d;
CREATE RELATION t_city (id) FOR DISTRIBUTION d;

\c regress

CREATE TABLE t_city(id int, name text, val text);

insert into t_city (id, name, val)
select * from (select *, 'x'::text as val from (select 1 as id, 'n' as name) as a) as b
returning id;

insert into t_city (id, name, val)
select * from (select *, case when 1 = 1 then 'y' else 'z' end as val from (select 12 as id, 'n' as name) as a) as b
returning id;

SELECT id FROM t_city WHERE id = 1 ORDER BY 1;
SELECT id FROM t_city WHERE id = 12 ORDER BY 1;

DROP TABLE t_city;

DELETE FROM spqr_metadata.spqr_distributed_relations /* __spqr__scatter_query: true */;

/* __spqr__execute_on: sh1 */ SELECT * FROM spqr_metadata.spqr_distributed_relations;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;