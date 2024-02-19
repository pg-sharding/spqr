\c spqr-console

CREATE DISTRIBUTION ds1 COLUMN TYPES integer;
CREATE KEY RANGE krid1 FROM 1 ROUTE TO sh1 FOR DISTRIBUTION ds1;
CREATE KEY RANGE krid2 FROM 101 ROUTE TO sh2 FOR DISTRIBUTION ds1;
ALTER DISTRIBUTION ds1 ATTACH RELATION tsa_test DISTRIBUTION KEY id;

\c regress
CREATE TABLE tsa_test (id int);
INSERT INTO tsa_test (id) VALUES (22);

-- you could specify target-session-attrs anywhere in your query
SELECT pg_is_in_recovery() /* target-session-attrs: read-write */ , id FROM tsa_test WHERE id = 22;
/* target-session-attrs: read-write */ SELECT pg_is_in_recovery(), id FROM tsa_test WHERE id = 22;
SELECT pg_is_in_recovery(), id FROM tsa_test WHERE id = 22 /* target-session-attrs: read-write */;

-- read-only is also supported but there is no high availability cluster in our tests yet, so it returns error
-- SELECT pg_is_in_recovery() /* target-session-attrs: read-only */ , id FROM tsa_test WHERE id = 22;
-- NOTICE: send query to shard(s) : sh1
-- ERROR:  failed to find replica

DROP TABLE tsa_test;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;