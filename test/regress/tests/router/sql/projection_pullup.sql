\c spqr-console
-- projection in target list pullup:
-- INSERT ... SELECT with nested subselects that project a column by name
-- must route by the resolved underlying value instead of failing to build
-- insert plan
CREATE DISTRIBUTION d (int);
CREATE KEY RANGE k2 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION d;
CREATE KEY RANGE k1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION d;
CREATE RELATION t_city (id) FOR DISTRIBUTION d;

\c regress

CREATE TABLE t_city(id int, val text);

insert into t_city(id) select * from (select aa from (select 1 as aa) a) b;
insert into t_city(id) select * from (select aa from (select 12 as aa) a) b;
insert into t_city(id) select * from (select * from (select 1 as aa) a) b;
insert into t_city(id) select * from (select * from (select 12 as aa) a) b;
insert into t_city(id) select aa from (select * from (select 1 as aa) a) b;
insert into t_city(id) select aa from (select * from (select 12 as aa) a) b;
insert into t_city(id) select aa from (select aa from (select 1 as aa) a) b;
insert into t_city(id) select aa from (select aa from (select 12 as aa) a) b;
insert into t_city(id, val) select aa, 'x' as bb from (select 12 as aa) a;

SELECT id FROM t_city WHERE id = 1 ORDER BY 1;
SELECT id FROM t_city WHERE id = 12 ORDER BY 1;

DROP TABLE t_city;

DELETE FROM spqr_metadata.spqr_distributed_relations /* __spqr__scatter_query: true */;

/* __spqr__execute_on: sh1 */ SELECT * FROM spqr_metadata.spqr_distributed_relations;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;