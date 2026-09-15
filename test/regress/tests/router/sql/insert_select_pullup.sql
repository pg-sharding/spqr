\c spqr-console
-- pullup sublist in simple single level select:
-- INSERT ... SELECT with nested subselects must route by the pulled-up
-- target list value instead of failing / misrouting
CREATE DISTRIBUTION d (int);
CREATE KEY RANGE k2 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION d;
CREATE KEY RANGE k1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION d;
CREATE RELATION t_city (id) FOR DISTRIBUTION d;

\c regress

CREATE TABLE t_city(id int);

insert into t_city(id) select * from (select * from (select 1));
insert into t_city(id) select * from (select 1);
insert into t_city(id) select 1;

-- routed to the other key range
insert into t_city(id) select * from (select * from (select 12));
insert into t_city(id) select 12;

SELECT id FROM t_city WHERE id = 1 ORDER BY 1;
SELECT id FROM t_city WHERE id = 12 ORDER BY 1;

DROP TABLE t_city;

DELETE FROM spqr_metadata.spqr_distributed_relations /* __spqr__scatter_query: true */;

/* __spqr__execute_on: sh1 */ SELECT * FROM spqr_metadata.spqr_distributed_relations;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;