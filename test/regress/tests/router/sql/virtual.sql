\c spqr-console

CREATE DISTRIBUTION ds1 COLUMN TYPES integer;

CREATE KEY RANGE FROM 301 ROUTE TO sh4 FOR DISTRIBUTION ds1;
CREATE KEY RANGE FROM 201 ROUTE TO sh3 FOR DISTRIBUTION ds1;

CREATE KEY RANGE FROM 101 ROUTE TO sh2 FOR DISTRIBUTION ds1;
CREATE KEY RANGE FROM 1 ROUTE TO sh1 FOR DISTRIBUTION ds1;

\c regress

SELECT 1;

SELECT 1, 2, 3;

SELECT 'postgres is cool';

SET __spqr__target_session_attrs to 'read-only';

SELECT pg_is_in_recovery();

SELECT __spqr__is_ready();

SELECT current_setting('transaction_read_only');

SELECT pg_is_in_recovery(), 1, 'kek';

SELECT CURRENT_USER;

select pg_is_in_recovery(), not pg_is_in_recovery(), __spqr__is_ready(), 1, 'a';

set __spqr__preferred_engine to v2;

select __spqr__shards();

select __spqr__show('key_ranges');

--- XXX: support
--- SELECT 1,2,3 UNION ALL SELECT 2,3,4;

-- XXX:support
--SHOW transaction_read_only;

-- XXX: support ignore patterns to test this
-- SELECT now(), 1;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;

