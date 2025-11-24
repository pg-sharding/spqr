\c spqr-console

-- check that numeric type works
CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
CREATE KEY RANGE FROM 300 ROUTE TO sh4 FOR DISTRIBUTION ds1;
CREATE KEY RANGE FROM 200 ROUTE TO sh3 FOR DISTRIBUTION ds1;
CREATE KEY RANGE FROM 100 ROUTE TO sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;

CREATE DISTRIBUTED RELATION xx_insert_rel DISTRIBUTION KEY a IN ds1;

\c regress

CREATE TABLE xx_insert_rel(a INT, b INT, c INT);


INSERT INTO xx_insert_rel (a, b, c) VALUES(1,2,3),(2,3,4), (3,4,5);
INSERT INTO xx_insert_rel (a, b, c) VALUES(1,2,3),(2,3,4), (300,4,5);
INSERT INTO xx_insert_rel (a, b, c) VALUES(100,2,3),(201,3,4), (301,4,5) ON CONFLICT DO NOTHING;
INSERT INTO xx_insert_rel (a, b, c) VALUES(200,2,3),(201,3,4), (301,4,5) RETURNING *;
INSERT INTO xx_insert_rel (a, b, c) SELECT 1,2,3;
INSERT INTO xx_insert_rel (a, b, c) SELECT 101,201,301;
INSERT INTO xx_insert_rel (a, b, c) SELECT 201,a,301 FROM unnest(ARRAY[110]) a;;
--INSERT INTO xx_insert_rel (a, b, c) SELECT 1,2,3 UNION ALL SELECT 2,3,4;


SELECT * FROM xx_insert_rel ORDER BY 1,2,3 /* __spqr__execute_on: sh1 */;
SELECT * FROM xx_insert_rel ORDER BY 1,2,3 /* __spqr__execute_on: sh2 */;
SELECT * FROM xx_insert_rel ORDER BY 1,2,3 /* __spqr__execute_on: sh3 */;
SELECT * FROM xx_insert_rel ORDER BY 1,2,3 /* __spqr__execute_on: sh4 */;

explain (COSTS OFF ) SELECT * FROM xx_insert_rel WHERE a = 1 ORDER BY 1,2,3;

DROP TABLE xx_insert_rel;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;