
SELECT __spqr__console_execute('CREATE DISTRIBUTION d(int)');
SELECT __spqr__console_execute('CREATE KEY RANGE k3 FROM 3000 ROUTE TO sh4');
SELECT __spqr__console_execute('CREATE KEY RANGE k2 FROM 2000 ROUTE TO sh3');
SELECT __spqr__console_execute('CREATE KEY RANGE k1 FROM 1000 ROUTE TO sh2');
SELECT __spqr__console_execute('CREATE KEY RANGE k0 FROM 0 ROUTE TO sh1');
SELECT __spqr__console_execute('CREATE RELATION flux_access_t1(i)');

-- xxx: fix this
INSERT INTO spqr_metadata.spqr_local_key_ranges (spqr_distribution, key_range_id, lower_bound) VALUES ('TODO', 'k0', 0) ON CONFLICT (key_range_id) DO NOTHING /* __spqr__execute_on: sh1 */;

CREATE TABLE flux_access_t1(i INT);

SELECT * FROM flux_access_t1 WHERE i = 67;

SELECT __spqr__console_execute('LOCK KEY RANGE k0');

-- should fail
SELECT * FROM flux_access_t1 WHERE i = 67;
INSERT INTO flux_access_t1(i) VALUES(67);

SET __spqr__flux_access TO true;
SHOW __spqr__flux_access;

SELECT * FROM flux_access_t1 WHERE i = 67;
-- should fail
INSERT INTO flux_access_t1(i) VALUES(67);

DROP TABLE flux_access_t1;
DELETE FROM spqr_metadata.spqr_local_key_ranges  /* __spqr__execute_on: sh1 */;

SELECT __spqr__console_execute('UNLOCK KEY RANGE k0; DROP DISTRIBUTION ALL CASCADE');