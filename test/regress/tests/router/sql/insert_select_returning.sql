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

WITH vals AS (
    SELECT * FROM (VALUES (2, 'n', 'x'), (13, 'm', 'y')) v(id, name, val)
), ins AS (
    INSERT INTO t_city (id, name, val)
    SELECT * FROM (SELECT * FROM vals) q
    RETURNING *
)
SELECT * FROM (SELECT * FROM ins) q;

SELECT id FROM t_city WHERE id IN (2, 13) /* __spqr__execute_on: sh1 */;
SELECT id FROM t_city WHERE id IN (2, 13) /* __spqr__execute_on: sh2 */;

INSERT INTO t_city (id, name, val)
SELECT * FROM (VALUES (3, 'n', 'x'), (14, 'n', 'y')) v(id, name, val) FETCH FIRST 1 ROW ONLY;
INSERT INTO t_city (id, name, val)
SELECT DISTINCT ON (name) * FROM (VALUES (3, 'n', 'x'), (14, 'n', 'y')) v(id, name, val);

SELECT id FROM t_city WHERE id = 1 ORDER BY 1;
SELECT id FROM t_city WHERE id = 12 ORDER BY 1;

DROP TABLE t_city;

DELETE FROM spqr_metadata.spqr_distributed_relations /* __spqr__scatter_query: true */;

/* __spqr__execute_on: sh1 */ SELECT * FROM spqr_metadata.spqr_distributed_relations;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;