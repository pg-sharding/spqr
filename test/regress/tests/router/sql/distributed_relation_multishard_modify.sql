\c spqr-console
CREATE DISTRIBUTION ds1 COLUMN TYPES int;
CREATE KEY RANGE krid2 FROM 30 ROUTE TO sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid1 FROM 1 ROUTE TO sh1 FOR DISTRIBUTION ds1;
ALTER DISTRIBUTION ds1 ATTACH RELATION distrr_mm_test DISTRIBUTION KEY id;

\c regress
CREATE TABLE distrr_mm_test (id INTEGER, t TEXT);

COPY distrr_mm_test(id,t) FROM STDIN;
1	'u'
2	'u'
3	'u'
4	'u'
5	'u'
\.

COPY distrr_mm_test(id,t) FROM STDIN;
31	'u'
32	'u'
33	'u'
34	'u'
35	'u'
\.


UPDATE distrr_mm_test SET t = 'm' WHERE id IN (3, 34) /* __spqr__engine_v2: true */;


SELECT * FROM distrr_mm_test ORDER BY id, t /*__spqr__execute_on: sh1 */;
SELECT * FROM distrr_mm_test ORDER BY id, t /*__spqr__execute_on: sh2 */;

DELETE FROM distrr_mm_test  WHERE id IN (2, 35) /* __spqr__engine_v2: true */;

SELECT * FROM distrr_mm_test ORDER BY id, t /*__spqr__execute_on: sh1 */;
SELECT * FROM distrr_mm_test ORDER BY id, t /*__spqr__execute_on: sh2 */;

-- This insert should succeed regardless of engine V2

INSERT INTO distrr_mm_test VALUES (1, 'zz'), (2, 'xx') /* __spqr__engine_v2: false*/;
INSERT INTO distrr_mm_test VALUES (1, 'zz'), (2, 'xx') /* __spqr__engine_v2: true */;

-- This insert should fail even with engine V2

INSERT INTO distrr_mm_test VALUES (1, 'zz'), (32, 'xx')/* __spqr__engine_v2: false */;
INSERT INTO distrr_mm_test VALUES (1, 'zz'), (32, 'xx') /* __spqr__engine_v2: true */;

DROP TABLE distrr_mm_test;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
