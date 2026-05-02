\c spqr-console

-- check that numeric type works
CREATE DISTRIBUTION ds1 (integer HASH);

CREATE KEY RANGE FROM 3221225472 ROUTE TO sh4;
CREATE KEY RANGE FROM 2147483648 ROUTE TO sh3;
CREATE KEY RANGE FROM 1073741824 ROUTE TO sh2;
CREATE KEY RANGE FROM 0 ROUTE TO sh1;

CREATE DISTRIBUTED RELATION xx_insert_rel_hash(a HASH MURMUR);

\c regress

CREATE TABLE xx_insert_rel_hash(a INT, b INT, c INT);

INSERT INTO xx_insert_rel_hash (a, b, c) VALUES(1,2,3),(2,3,4), (3,4,5);
INSERT INTO xx_insert_rel_hash (a, b, c) VALUES(1,2,3),(2,3,4), (300,4,5) /*__spqr__engine_v2: false */;
INSERT INTO xx_insert_rel_hash (a, b, c) VALUES(100,2,3),(201,3,4), (301,4,5) ON CONFLICT DO NOTHING /*__spqr__engine_v2: false */;
INSERT INTO xx_insert_rel_hash (a, b, c) VALUES(200,2,3),(201,3,4), (301,4,5) RETURNING * /*__spqr__engine_v2: false */;

SELECT a FROM xx_insert_rel_hash WHERE a IN (100, 301, 304);

SELECT __spqr__ctid('xx_insert_rel_hash');

TRUNCATE xx_insert_rel_hash;

-- check columns order

INSERT INTO xx_insert_rel_hash (b, a, c) VALUES(1,2,3),(2,3,4), (3,4,5);
INSERT INTO xx_insert_rel_hash (b, c, a) VALUES(1,2,3),(2,3,4), (300,4,5) /*__spqr__engine_v2: false */;
INSERT INTO xx_insert_rel_hash (c, b, a) VALUES(100,202,303),(207,304,204), (307,104,5) ON CONFLICT DO NOTHING /*__spqr__engine_v2: false */;
INSERT INTO xx_insert_rel_hash (c, a, b) VALUES(200,202,303),(207,304,204), (307,104,5) RETURNING * /*__spqr__engine_v2: false */;

SELECT __spqr__ctid('xx_insert_rel_hash');

TRUNCATE xx_insert_rel_hash;

BEGIN;
INSERT INTO xx_insert_rel_hash (a, b, c) VALUES(200,2,3),(201,3,4), (301,4,5) RETURNING * /*__spqr__engine_v2: true */;
ROLLBACK;

INSERT INTO xx_insert_rel_hash (a, b, c) SELECT 1,2,3;
INSERT INTO xx_insert_rel_hash (a, b, c) SELECT 101,201,301;
INSERT INTO xx_insert_rel_hash (a, b, c) SELECT 201,a,301 FROM unnest(ARRAY[110]) a;
--INSERT INTO xx_insert_rel_hash (a, b, c) SELECT 1,2,3 UNION ALL SELECT 2,3,4;

SELECT __spqr__ctid('xx_insert_rel_hash');

explain (COSTS OFF ) SELECT * FROM xx_insert_rel_hash WHERE a = 1 ORDER BY 1,2,3;

DROP TABLE xx_insert_rel_hash;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;