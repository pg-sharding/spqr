
SELECT 1;

SELECT 1, 2, 3;

SELECT 'postgres is cool';

SELECT pg_is_in_recovery();

SELECT current_setting('transaction_read_only');

SELECT pg_is_in_recovery(), 1, 'kek';

-- XXX:support
--SHOW transaction_read_only;

-- XXX: support ignore patterns to test this
-- SELECT now(), 1;
