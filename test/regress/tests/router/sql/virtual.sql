
SELECT 1;

SELECT pg_is_in_recovery();

SELECT current_setting('transaction_read_only');

SHOW transaction_read_only;

SELECT now(), 1;
