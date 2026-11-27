CREATE TABLE pool_rollback_test (i int);
BEGIN;
-- Make SELECT after reconnect wait for the first transaction to finish.
TRUNCATE pool_rollback_test;
INSERT INTO pool_rollback_test VALUES (42);
\c
SELECT * FROM pool_rollback_test;
DROP TABLE pool_rollback_test;
