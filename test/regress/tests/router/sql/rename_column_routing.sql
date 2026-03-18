\c spqr-console

CREATE DISTRIBUTION ds_rc COLUMN TYPES integer;

CREATE KEY RANGE rc_kr2 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION ds_rc;
CREATE KEY RANGE rc_kr1 FROM 1 ROUTE TO sh1 FOR DISTRIBUTION ds_rc;

-- Attach with wrong casing (the real-world use case for RENAME)
ALTER DISTRIBUTION ds_rc ATTACH RELATION rc_test DISTRIBUTION KEY W_ID;

SHOW relations;

\c regress

-- INSERT fails: router can't match column "w_id" to metadata "W_ID"
INSERT INTO rc_test (w_id, val) VALUES (1, 'row_sh1');

-- SELECT scatters to all shards: column mismatch
SELECT * FROM rc_test WHERE w_id = 1;

-- Fix the metadata: W_ID -> w_id
\c spqr-console
ALTER DISTRIBUTION ds_rc ALTER RELATION rc_test RENAME DISTRIBUTION COLUMN W_ID TO w_id;
SHOW relations;

\c regress

-- After fix, routing targets correct shard
CREATE TABLE rc_test (w_id int, val text);

INSERT INTO rc_test (w_id, val) VALUES (1, 'row_sh1');
INSERT INTO rc_test (w_id, val) VALUES (20, 'row_sh2');

SELECT * FROM rc_test WHERE w_id = 1;
SELECT * FROM rc_test WHERE w_id = 20;

DROP TABLE rc_test;

-- Expression routing: rename must be rejected
\c spqr-console

CREATE DISTRIBUTION ds_expr COLUMN TYPES int hash;
ALTER DISTRIBUTION ds_expr ATTACH RELATION expr_test DISTRIBUTION KEY MURMUR [id1 INT HASH, id2 VARCHAR HASH];
ALTER DISTRIBUTION ds_expr ALTER RELATION expr_test RENAME DISTRIBUTION COLUMN id1 TO new_id;

DROP DISTRIBUTION ALL CASCADE;
