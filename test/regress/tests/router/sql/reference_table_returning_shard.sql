\c spqr-console

-- Regression test: INSERT ... RETURNING on a reference relation must return
-- data rows from the shard the reference relation is stored on, not from
-- the first shard routed inside transaction

CREATE REFERENCE TABLE t ON sh1;

\c regress

CREATE TABLE t(id int, name text);

BEGIN;

-- route to sh2 first, so the first slot of the shared multi shard
-- gang is occupied by sh2, not by the reference relation storage shard

SELECT * FROM t /* __spqr__execute_on: sh2 */;

INSERT INTO t (id, name) VALUES (1, 'test1') RETURNING id, name;

ROLLBACK;

-- after rollback data should be gone

SELECT id, name FROM t /* __spqr__execute_on: sh1 */;
SELECT id, name FROM t /* __spqr__execute_on: sh2 */;

DROP TABLE t;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
